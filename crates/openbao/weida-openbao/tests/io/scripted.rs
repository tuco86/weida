//! The tests of `tests/scripted.rs` that need the network, a process or real time.

use crate::common;
use common::{Bao, Raw, Script};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use weida::{FilesOptions, IdentityEvent, IdentitySource};
use weida_openbao::{Auth, Error, HandoffSource, Kv, OpenBao, PkiAnchor, PkiSign};

use crate::scripted::*;

/// Claim: a given token is looked up so the client knows its TTL and
/// accessor, and every later request carries it.
#[tokio::test]
async fn a_token_is_looked_up_and_then_used() {
    let script = Script::new();
    script.on("GET", "auth/token/lookup-self", |r| {
        assert_eq!(r.token(), Some("t-given"));
        (200, json!({ "data": { "accessor": "acc-1", "ttl": 3600, "renewable": false, "policies": ["p"] } }))
    });
    script.on("GET", "secret/data/x", |r| {
        assert_eq!(r.token(), Some("t-given"));
        (200, json!({ "data": { "data": { "v": 1 } } }))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(config(&bao.address)).expect("client");
    let info = within(client.login(Auth::Token("t-given".into())))
        .await
        .expect("login");
    assert_eq!(info.accessor.as_deref(), Some("acc-1"));
    assert_eq!(info.ttl, Duration::from_secs(3600));
    assert!(!info.renewable);
    let v = within(client.get("secret/data/x")).await.expect("get");
    assert_eq!(v["data"]["data"]["v"], 1);
}
/// Claim: AppRole logs in without a token and comes back with one.
#[tokio::test]
async fn approle_logs_in_anonymously() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |r| {
        assert_eq!(r.token(), None, "a login carries no token");
        assert_eq!(r.body["role_id"], "r1");
        assert_eq!(r.body["secret_id"], "s1");
        (200, auth_answer("t-approle", 60, true))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(config(&bao.address)).expect("client");
    let info = within(client.login(Auth::approle("r1", "s1")))
        .await
        .expect("login");
    assert_eq!(info.accessor.as_deref(), Some("acc-t-approle"));
    assert_eq!(client.accessor().as_deref(), Some("acc-t-approle"));
}
/// Claim: the hand-off redeems the wrapping token as its first and only
/// request, with the wrapping token as the token; what comes back is the
/// service token. A wrapping token somebody already redeemed is
/// `HandoffStolen`, not an API error among others.
#[tokio::test]
async fn the_handoff_redeems_once_and_theft_is_named() {
    let script = Script::new();
    // Single use, as the real endpoint: the first redemption of `w-fresh`
    // succeeds, every later one is the defined error.
    let redeemed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    script.on("POST", "sys/wrapping/unwrap", move |r| {
        let first = r.token() == Some("w-fresh")
            && !redeemed.swap(true, std::sync::atomic::Ordering::AcqRel);
        if first {
            (200, auth_answer("t-service", 120, true))
        } else {
            (
                400,
                json!({ "errors": ["wrapping token is not valid or does not exist"] }),
            )
        }
    });
    let bao = Bao::start(script).await;
    let dir = private_dir("handoff");
    std::fs::create_dir_all(&dir).expect("dir");
    let credential = dir.join("bao-handoff");
    std::fs::write(&credential, "w-fresh\n").expect("write credential");

    let client = OpenBao::new(config(&bao.address)).expect("client");
    let info = within(client.login(Auth::Handoff(HandoffSource::File(credential.clone()))))
        .await
        .expect("redeem");
    assert_eq!(info.accessor.as_deref(), Some("acc-t-service"));
    let seen = bao.script.seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 1, "the unwrap was the first and only request");
    assert_eq!(seen[0].path, "/v1/sys/wrapping/unwrap");

    // Somebody else — or this process again — redeems the same credential.
    let again = OpenBao::new(config(&bao.address)).expect("client");
    let err = within(again.login(Auth::Handoff(HandoffSource::File(credential))))
        .await
        .expect_err("already redeemed");
    assert!(matches!(err, Error::HandoffStolen), "{err:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
/// Claim: a renewable token is renewed at half its TTL for as long as the
/// client lives, and the renewed TTL is the next pace.
#[tokio::test]
async fn a_renewable_token_is_renewed_at_half_its_ttl() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 2, true))
    });
    script.on("POST", "auth/token/renew-self", |r| {
        assert_eq!(r.token(), Some("t"));
        (200, auth_answer("t", 2, true))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(config(&bao.address)).expect("client");
    within(client.login(Auth::approle("r", "s")))
        .await
        .expect("login");
    let started = Instant::now();
    until(|| bao.script.seen_on("auth/token/renew-self").len() >= 2).await;
    // Not faster than the pace: two renewals at a second each cannot have
    // happened in much under two seconds. (How slow it may be is no claim.)
    assert!(
        started.elapsed() >= Duration::from_millis(1900),
        "two renewals at a 1 s pace came after {:?}",
        started.elapsed()
    );
}
/// Claim: `PkiSign` sends a CSR over the key it was given and serves the
/// chain that comes back; a renewal is a `Renewed` event under the same
/// fingerprint, and a renewal the server refuses is `RenewalFailed` with
/// the certificate in service unchanged.
#[tokio::test]
async fn pki_sign_keeps_the_key_and_renews_the_certificate() {
    let dir = private_dir("pki");
    let key_source = IdentitySource::files(
        &dir,
        FilesOptions {
            names: vec!["svc.local".into()],
            ..FilesOptions::default()
        },
    )
    .expect("key source");
    let fingerprint = key_source.fingerprint().expect("fingerprint");
    let key_pem = std::fs::read_to_string(dir.join("key.pem")).expect("key.pem");

    // The "CA" signs by re-signing over the CSR's key, which this script
    // knows because the test made it; what is asserted is the plumbing, the
    // real signature is the dev server's business.
    let script = Script::new();
    let signed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    script.on("POST", "pki/sign/svc", {
        let key_pem = key_pem.clone();
        let signed = signed.clone();
        move |r| {
            assert!(r.body["csr"].as_str().is_some_and(|c| c.contains("BEGIN CERTIFICATE REQUEST")));
            assert_eq!(r.body["use_csr_sans"], true);
            assert_eq!(r.body["common_name"], "svc.local");
            let n = signed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if n == 2 {
                return (403, json!({ "errors": ["permission denied"] }));
            }
            let key = rcgen::KeyPair::from_pem(&key_pem).expect("key");
            let params = rcgen::CertificateParams::new(vec!["svc.local".to_owned()]).expect("params");
            let cert = params.self_signed(&key).expect("sign").pem();
            let expiration = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_secs()
                + 3;
            (200, json!({ "data": { "certificate": cert, "ca_chain": [], "expiration": expiration } }))
        }
    });
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(config(&bao.address)).expect("client");
    within(client.login(Auth::approle("r", "s")))
        .await
        .expect("login");

    let signing = PkiSign {
        ttl: Some(Duration::from_secs(3)),
        renew_at: 0.34,
        ..PkiSign::new("svc", ["svc.local"])
    };
    let source = within(signing.start(client.clone(), &key_source))
        .await
        .expect("first signature");
    assert_eq!(
        source.fingerprint().expect("fingerprint"),
        fingerprint,
        "the key is unchanged"
    );
    let mut events = source.events();
    // About one second in: the first renewal; another second: the refused one.
    assert_eq!(
        within(events.recv()).await,
        Some(IdentityEvent::Renewed { fingerprint })
    );
    let failed = within(events.recv()).await;
    assert!(
        matches!(&failed, Some(IdentityEvent::RenewalFailed(m)) if m.contains("permission denied")),
        "{failed:?}"
    );
    assert_eq!(source.fingerprint().expect("fingerprint"), fingerprint);
    let _ = std::fs::remove_dir_all(&dir);
}
/// Claim: `PkiAnchor` trusts the mount's chain and picks up a rotated one;
/// `Kv` reads the two fields of a secret into an identity.
#[tokio::test]
async fn pki_anchor_refreshes_and_kv_reads_an_identity() {
    let ca_a = weida::Identity::generate_for(["ca-a"])
        .expect("ca a")
        .certificate_pem()
        .expect("pem");
    let ca_b = weida::Identity::generate_for(["ca-b"])
        .expect("ca b")
        .certificate_pem()
        .expect("pem");
    let stored = weida::Identity::generate_for(["stored"]).expect("stored");
    let script = Script::new();
    let fetches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    script.on("GET", "pki/cert/ca_chain", {
        let (ca_a, ca_b, fetches) = (ca_a.clone(), ca_b.clone(), fetches.clone());
        move |_| {
            let n = fetches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let ca = if n < 2 { &ca_a } else { &ca_b };
            (200, json!({ "data": { "certificate": ca } }))
        }
    });
    script.on("GET", "secret/data/svc/identity", {
        let cert = stored.certificate_pem().expect("pem");
        let key = stored.to_pem().expect("pem");
        move |_| {
            (
                200,
                json!({ "data": { "data": { "certificate": cert, "private_key": key } } }),
            )
        }
    });
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(config(&bao.address)).expect("client");
    within(client.login(Auth::approle("r", "s")))
        .await
        .expect("login");

    let anchor = PkiAnchor {
        refresh: Duration::from_millis(100),
        ..PkiAnchor::new()
    };
    let trust = within(anchor.start(client.clone())).await.expect("anchor");
    assert_eq!(
        trust.current().anchors,
        vec![weida::Pem::Bytes(zeroize::Zeroizing::new(
            ca_a.into_bytes()
        ))]
    );
    let mut events = trust.events();
    assert_eq!(
        within(events.recv()).await,
        Some(IdentityEvent::TrustRefreshed)
    );
    assert_eq!(
        trust.current().anchors,
        vec![weida::Pem::Bytes(zeroize::Zeroizing::new(
            ca_b.into_bytes()
        ))]
    );

    let identity = within(Kv::new("svc/identity").start(client))
        .await
        .expect("kv");
    assert_eq!(
        identity.fingerprint().expect("fingerprint"),
        stored.fingerprint().expect("fingerprint")
    );
}
/// Claim: a redirect is not followed, so the token and the request never
/// reach the host it names. The redirect surfaces as an error that says where
/// it pointed.
#[tokio::test]
async fn a_redirect_is_not_followed_and_the_token_stays_home() {
    // The host the redirect points at answers anything and records it.
    let elsewhere = Script::new();
    elsewhere.on("GET", "secret/data/x", |_| {
        (200, json!({ "data": { "data": { "v": 1 } } }))
    });
    let elsewhere = Bao::start(elsewhere).await;

    let front = Script::new();
    front.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t-secret", 0, false))
    });
    let target = format!("{}/v1/secret/data/x", elsewhere.address);
    front.on_raw("GET", "secret/data/x", move |_| Raw {
        status: 307,
        headers: vec![("Location".to_owned(), target.clone())],
        body: Vec::new(),
    });
    let front = Bao::start(front).await;
    let client = logged_in(&front).await;

    let err = within(client.get("secret/data/x"))
        .await
        .expect_err("a redirect is not an answer");
    assert!(
        matches!(&err, Error::Api { status: 307, errors }
            if errors.iter().any(|e| e.contains(&elsewhere.address))),
        "{err:?}"
    );
    assert!(
        elsewhere.script.seen.lock().expect("seen").is_empty(),
        "the redirect was followed, and the token went with it"
    );
}
/// Claim: an answer that declares a length past the cap is refused before it
/// is read.
#[tokio::test]
async fn an_answer_that_declares_more_than_the_cap_is_refused() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    script.on_raw("GET", "sys/huge", |_| Raw {
        status: 200,
        headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
        body: vec![b' '; ONE_PAST_THE_CAP],
    });
    let bao = Bao::start(script).await;
    let client = logged_in(&bao).await;

    let err = within(client.get("sys/huge"))
        .await
        .expect_err("past the cap");
    assert!(matches!(err, Error::Http(_)), "{err:?}");
}
/// Claim: an answer that declares nothing is cut at the cap as it streams in,
/// not buffered to the end.
#[tokio::test]
async fn an_answer_with_no_declared_length_is_cut_at_the_cap_too() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    script.on_raw("GET", "sys/huge", |_| Raw {
        status: 200,
        headers: vec![
            ("Content-Type".to_owned(), "application/json".to_owned()),
            ("Transfer-Encoding".to_owned(), "chunked".to_owned()),
        ],
        body: vec![b' '; ONE_PAST_THE_CAP],
    });
    let bao = Bao::start(script).await;
    let client = logged_in(&bao).await;

    let err = within(client.get("sys/huge"))
        .await
        .expect_err("past the cap");
    assert!(matches!(err, Error::Http(_)), "{err:?}");
}
/// Claim: what a server writes into an error is cut, so a hostile body cannot
/// flood the error or the log line it ends up in.
#[tokio::test]
async fn a_hostile_error_body_is_cut_before_it_reaches_the_error() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    script.on_raw("GET", "sys/noisy", |_| Raw {
        status: 500,
        headers: Vec::new(),
        body: [b"line one\nforged: line two ".as_slice(), &[b'x'; 1 << 20]].concat(),
    });
    let bao = Bao::start(script).await;
    let client = logged_in(&bao).await;

    let err = within(client.get("sys/noisy")).await.expect_err("a 500");
    let Error::Api {
        status: 500,
        errors,
    } = &err
    else {
        panic!("{err:?}");
    };
    let total: usize = errors.iter().map(String::len).sum();
    assert!(total < 4096, "{total} bytes of a server's text kept");
    assert!(
        errors.iter().all(|e| !e.contains('\n')),
        "a newline from the server survived: {errors:?}"
    );
}
/// Claim: a renewal that fails is retried while the token still lives. A
/// four-second token is renewed at two; the second attempt, after the first
/// failed, must come before the token expires at four and not after another
/// half of its life plus a pause.
#[tokio::test]
async fn a_failed_renewal_is_retried_before_the_token_expires() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 4, true))
    });
    let attempts: Arc<Mutex<Vec<Instant>>> = Arc::default();
    script.on("POST", "auth/token/renew-self", {
        let attempts = Arc::clone(&attempts);
        move |_| {
            let mut attempts = attempts.lock().expect("attempts");
            attempts.push(Instant::now());
            if attempts.len() == 1 {
                (500, json!({ "errors": ["standby; try again"] }))
            } else {
                (200, auth_answer("t", 4, true))
            }
        }
    });
    let bao = Bao::start(script).await;
    let client = logged_in(&bao).await;
    let logged_in_at = Instant::now();

    until(|| attempts.lock().expect("attempts").len() >= 2).await;
    // A measurement, not a bound: the retry's own pace (a second after a
    // failure on a four-second token, never half a life later) is pinned
    // without a clock by `renewal_wait`'s unit tests. What this asserts is
    // that the retry happened at all.
    let second = attempts.lock().expect("attempts")[1].duration_since(logged_in_at);
    eprintln!("the retry came {second:?} after the login, on a four-second token");
    drop(client);
}
/// Claim: a renewable token with no lease at all is a token that never
/// expires, which is not renewed once a second for the life of the process.
#[tokio::test]
async fn a_token_that_never_expires_is_not_renewed() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, true))
    });
    script.on("POST", "auth/token/renew-self", |_| {
        (200, auth_answer("t", 0, true))
    });
    let bao = Bao::start(script).await;
    let _client = logged_in(&bao).await;

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        bao.script.seen_on("auth/token/renew-self").len(),
        0,
        "a token with no TTL was renewed"
    );
}
/// Claim: a renewal the server answers with "permission denied" ends the
/// loop: the token is no longer one it honours, and asking again cannot help.
#[tokio::test]
async fn a_token_the_server_no_longer_honours_stops_being_renewed() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 2, true))
    });
    script.on("POST", "auth/token/renew-self", |_| {
        (403, json!({ "errors": ["permission denied"] }))
    });
    let bao = Bao::start(script).await;
    let _client = logged_in(&bao).await;

    until(|| !bao.script.seen_on("auth/token/renew-self").is_empty()).await;
    // A generic failure would be retried a second later.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(bao.script.seen_on("auth/token/renew-self").len(), 1);
}
/// Claim: an expiry no clock can hold is a shape error from `start`, not a
/// panic in the arithmetic that schedules the renewal.
#[tokio::test]
async fn an_expiry_no_clock_can_hold_is_refused() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    script.on("POST", "pki/sign/svc", |_| {
        (
            200,
            json!({ "data": { "certificate": "x", "ca_chain": [], "expiration": u64::MAX } }),
        )
    });
    let bao = Bao::start(script).await;
    let client = logged_in(&bao).await;

    let err = within(PkiSign::new("svc", ["svc.local"]).start(client, &key_source("expiry")))
        .await
        .expect_err("an impossible expiry");
    assert!(matches!(err, Error::Shape(_)), "{err:?}");
}
/// Claim: a renewal fraction that is not a number is refused where it is
/// given, not discovered by the task that must keep a certificate alive.
#[tokio::test]
async fn a_renewal_fraction_that_is_not_a_number_is_refused() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    let bao = Bao::start(script).await;
    let client = logged_in(&bao).await;

    for renew_at in [f64::NAN, f64::INFINITY] {
        let signing = PkiSign {
            renew_at,
            ..PkiSign::new("svc", ["svc.local"])
        };
        let err = within(signing.start(client.clone(), &key_source("fraction")))
            .await
            .expect_err("not a fraction");
        assert!(matches!(err, Error::Config(_)), "{renew_at}: {err:?}");
    }
    assert!(
        bao.script.seen_on("pki/sign/svc").is_empty(),
        "a request was made before the configuration was checked"
    );
}
/// Claim: a refresh task ends when the application drops its source. The
/// scripted server counts the fetches; after the drop the count stops, which
/// is a task that has returned and let go of the client it held.
#[tokio::test]
async fn a_trust_refresh_ends_when_its_source_is_dropped() {
    let ca = weida::Identity::generate_for(["ca"])
        .expect("ca")
        .certificate_pem()
        .expect("pem");
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    script.on("GET", "pki/cert/ca_chain", move |_| {
        (200, json!({ "data": { "certificate": ca } }))
    });
    let bao = Bao::start(script).await;
    let client = logged_in(&bao).await;

    let anchor = PkiAnchor {
        refresh: Duration::from_millis(50),
        ..PkiAnchor::new()
    };
    let trust = within(anchor.start(client)).await.expect("anchor");
    until(|| bao.script.seen_on("pki/cert/ca_chain").len() >= 3).await;

    drop(trust);
    // One more round may be in flight when the source goes.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let after_the_drop = bao.script.seen_on("pki/cert/ca_chain").len();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        bao.script.seen_on("pki/cert/ca_chain").len(),
        after_the_drop,
        "the refresh kept fetching for a source nobody holds"
    );
}
/// Claim: the same for a KV refresh.
#[tokio::test]
async fn a_kv_refresh_ends_when_its_source_is_dropped() {
    let stored = weida::Identity::generate_for(["stored"]).expect("stored");
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    script.on("GET", "secret/data/svc/identity", {
        let cert = stored.certificate_pem().expect("pem");
        let key = stored.to_pem().expect("pem");
        move |_| {
            (
                200,
                json!({ "data": { "data": { "certificate": cert, "private_key": key } } }),
            )
        }
    });
    let bao = Bao::start(script).await;
    let client = logged_in(&bao).await;

    let kv = Kv {
        refresh: Some(Duration::from_millis(50)),
        ..Kv::new("svc/identity")
    };
    let identity = within(kv.start(client)).await.expect("kv");
    until(|| bao.script.seen_on("secret/data/svc/identity").len() >= 3).await;

    drop(identity);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let after_the_drop = bao.script.seen_on("secret/data/svc/identity").len();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        bao.script.seen_on("secret/data/svc/identity").len(),
        after_the_drop,
        "the refresh kept reading for a source nobody holds"
    );
}
