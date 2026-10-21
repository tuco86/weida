//! The connection on the wire: the handshake, the negotiated limits, the
//! keep-alive timer, and the reason code of a server DISCONNECT.
//!
//! Each test names the conformance statement it is about, because that is what
//! makes it a conformance test rather than a regression net.

use crate::harness;

use std::sync::Arc;
use std::time::Duration;

use weida_mqtt::{Client, ConnectOptions, Context, Error};

pub(crate) fn options() -> ConnectOptions {
    let mut options = harness::options("test-client");
    options.keep_alive = Duration::from_secs(60);
    options
}

/// An authenticator with no `authentication_method` would never be asked
/// anything: no AUTH exchange starts and `finish` is skipped, so a verifier
/// would verify nothing. It is refused before any socket is opened, which the
/// unreachable address proves: dialling it would fail with an I/O error.
#[tokio::test]
async fn an_authenticator_without_an_authentication_method_is_refused() {
    struct Verifier;
    impl weida_mqtt::Authenticator for Verifier {
        fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>, Error> {
            Ok(vec![1])
        }

        fn finish(&self, _data: Option<&[u8]>) -> Result<(), Error> {
            Err(Error::AuthenticationFailed("never reached".into()))
        }
    }

    let context = Context::new().expect("ambient");
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        Client::connect_with(&context, "127.0.0.1:1", options(), Arc::new(Verifier)),
    )
    .await
    .expect("the refusal is immediate")
    .expect_err("refused");
    assert!(matches!(error, Error::Configuration(_)), "{error}");
}

/// A zero timeout expires before the wait it bounds has begun, and one past
/// what an `Instant` can hold would panic in the connection task, so both are
/// refused where the connect starts.
#[tokio::test]
async fn a_timeout_that_cannot_be_a_deadline_is_refused_at_connect() {
    let context = Context::new().expect("ambient");
    for set in [
        (|options: &mut ConnectOptions| options.connect_timeout = Duration::ZERO)
            as fn(&mut ConnectOptions),
        |options| options.ping_timeout = Some(Duration::ZERO),
        |options| options.write_timeout = Some(Duration::MAX),
    ] {
        let mut options = options();
        set(&mut options);
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            Client::connect(&context, "127.0.0.1:1", options),
        )
        .await
        .expect("the refusal is immediate")
        .expect_err("refused");
        assert!(matches!(error, Error::Configuration(_)), "{error}");
    }
}
