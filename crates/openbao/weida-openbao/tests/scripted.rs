//! The client, the three auths and the three sources, against a scripted
//! OpenBao ([0032](../../../docs/decisions/0032-identity-sources-and-the-handoff.md)
//! §4.3). What the real server does with the same requests is
//! `tests/bao_dev.rs`, run by hand against the local binary.

use crate::common;

use std::time::Duration;

use common::Bao;
use serde_json::json;
use weida::{FilesOptions, IdentitySource};
use weida_openbao::{Auth, Config, Error, OpenBao};

/// One step of a test, expected within ten seconds. Past that the wait
/// reports and goes on instead of failing (`weida_testkit::within`).
#[track_caller]
pub(crate) fn within<F: Future>(f: F) -> impl Future<Output = F::Output> {
    let at = std::panic::Location::caller();
    async move {
        let what = format!("the step at {}:{}", at.file(), at.line());
        weida_testkit::within(&what, Duration::from_secs(10), f).await
    }
}

/// The scripted server speaks `http://` on this host, which the client only
/// does when told to.
pub(crate) fn config(address: &str) -> Config {
    Config {
        allow_insecure: true,
        ..Config::new(address)
    }
}

pub(crate) fn private_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "weida-bao-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

pub(crate) fn auth_answer(token: &str, ttl: u64, renewable: bool) -> serde_json::Value {
    json!({
        "auth": {
            "client_token": token,
            "accessor": format!("acc-{token}"),
            "lease_duration": ttl,
            "renewable": renewable,
            "policies": ["default", "svc"],
        }
    })
}

// --- what the client refuses to do ------------------------------------------

/// A logged-in client of `script`, which must answer the AppRole login.
pub(crate) async fn logged_in(bao: &Bao) -> OpenBao {
    let client = OpenBao::new(config(&bao.address)).expect("client");
    within(client.login(Auth::approle("r", "s")))
        .await
        .expect("login");
    client
}

/// The cap on one answer, which is the client's own and not a thing the
/// tests can name: one byte more than 16 MiB.
pub(crate) const ONE_PAST_THE_CAP: usize = (16 << 20) + 1;

/// Claim: the client speaks `https://` unless told otherwise, and only
/// `https://` and `http://`.
#[tokio::test]
async fn plain_http_is_refused_unless_it_is_allowed() {
    for refused in [
        "http://127.0.0.1:8200",
        "ftp://bao.example",
        "not a url",
        "",
    ] {
        let err = OpenBao::new(Config::new(refused)).expect_err(refused);
        assert!(matches!(err, Error::Config(_)), "{refused:?}: {err:?}");
    }
    let allowed = Config {
        allow_insecure: true,
        ..Config::new("http://127.0.0.1:8200")
    };
    OpenBao::new(allowed).expect("http, asked for by name");
    OpenBao::new(Config::new("https://bao.example:8200")).expect("https");
    // The option opens `http://` and nothing else.
    let err = OpenBao::new(Config {
        allow_insecure: true,
        ..Config::new("ftp://bao.example")
    })
    .expect_err("still not http");
    assert!(matches!(err, Error::Config(_)), "{err:?}");
}

// --- the token's renewal ----------------------------------------------------

/// Polls `done` until it holds; reports at ten seconds and goes on.
pub(crate) async fn until(mut done: impl FnMut() -> bool) {
    within(async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
}

// --- values the server or the configuration chooses -------------------------

pub(crate) fn key_source(tag: &str) -> IdentitySource {
    IdentitySource::files(
        private_dir(tag),
        FilesOptions {
            names: vec!["svc.local".into()],
            ..FilesOptions::default()
        },
    )
    .expect("key source")
}

// --- background tasks end with what they serve ------------------------------
