//! The `ipc` transport through the public API.
//!
//! Patterns are transport-blind, so this is not a second copy of the pattern
//! suites either (see `tests/inproc.rs`). What is asserted here is what only
//! `AF_UNIX` has: the socket file, the credentials an application can read
//! off a peer, the path budget, and the endpoint-stealing behaviour libzmq
//! documents and this library shares.

#![cfg(unix)]

use std::path::{Path, PathBuf};

use weida_zmq::{Context, ContextConfig, Endpoint, MAX_IPC_ENDPOINT_BYTES, Multipart};

pub(crate) fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

/// A socket path in the temporary directory, unique per test.
///
/// Honest for a test and wrong for a program: `/tmp` is world-writable, which
/// is the substitution hazard `weida_zmq::ipc` documents. A program puts its
/// socket in a directory it owns.
pub(crate) fn socket_path(what: &str) -> PathBuf {
    std::env::temp_dir().join(format!("weida-zmq-it-{}-{what}.sock", std::process::id()))
}

pub(crate) fn endpoint(path: &Path) -> String {
    format!("ipc://{}", path.display())
}

pub(crate) fn text(message: &Multipart) -> Vec<u8> {
    message.frames()[0].as_slice().to_vec()
}

pub(crate) async fn wait_for(done: impl FnMut() -> bool) {
    weida_testkit::until("the condition", weida_testkit::EXPECTED, done).await;
}

/// Claim: the endpoint string is bounded where libzmq bounds it — 113 bytes
/// including the prefix — so a program ported from libzmq is refused at the
/// same character, before anything touches the filesystem.
#[tokio::test]
async fn the_endpoint_budget_is_libzmqs() {
    let longest = "x".repeat(MAX_IPC_ENDPOINT_BYTES - "ipc:///".len());
    let ok = format!("ipc:///{longest}");
    assert_eq!(ok.len(), MAX_IPC_ENDPOINT_BYTES);
    assert!(Endpoint::parse(&ok).is_ok());

    let err = Endpoint::parse(&format!("{ok}y")).unwrap_err();
    assert_eq!(err.errno(), "EINVAL", "{err}");
}
