//! TLS on 8883, the CONNECT credentials, and the AUTH exchange of 4.12,
//! including re-authentication over a live connection, on the wire.
//!
//! The TLS test completes a **real** handshake against a certificate this test
//! generates, rather than asserting a type: the claim being made is that the
//! CONNECT travels inside TLS, and only a real handshake can carry it.

use crate::harness;

use std::sync::Arc;

use harness::bytes;
use weida_mqtt::{Authenticator, Client, ConnectOptions, Context, Error, Result};
use weida_mqtt_codec::{Packet, Properties};

pub(crate) fn options() -> ConnectOptions {
    harness::options("tls-auth-client")
}

/// An AUTH the server sends: a reason code, the method it repeats, and its
/// `Authentication Data`.
pub(crate) fn auth(code: u8, method: &str, data: &[u8]) -> Vec<u8> {
    bytes(&Packet::Auth(weida_mqtt_codec::Auth {
        reason_code: weida_mqtt_codec::AuthReasonCode::from_byte(code).expect("an AUTH code"),
        properties: Properties {
            authentication_method: Some(method),
            authentication_data: Some(data),
            ..Properties::new()
        },
    }))
}

/// An authenticator that answers every challenge with the challenge's own
/// bytes plus a counter, so a test can tell the opening move from the answers
/// and one round from the next.
pub(crate) struct Counting {
    calls: std::sync::atomic::AtomicUsize,
}

impl Counting {
    pub(crate) fn new() -> Arc<Counting> {
        Arc::new(Counting {
            calls: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Authenticator for Counting {
    fn challenge(&self, data: Option<&[u8]>) -> Result<Vec<u8>> {
        let round = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut answer = data.unwrap_or_default().to_vec();
        answer.push(u8::try_from(round).unwrap_or(u8::MAX));
        Ok(answer)
    }

    fn finish(&self, _data: Option<&[u8]>) -> Result<()> {
        Ok(())
    }
}

/// An authenticator that refuses, which is a client that cannot answer what
/// the server asked.
pub(crate) struct Refusing;

impl Authenticator for Refusing {
    fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>> {
        Err(Error::Configuration(
            "no credential for this challenge".into(),
        ))
    }

    fn finish(&self, _data: Option<&[u8]>) -> Result<()> {
        Ok(())
    }
}

/// A `server_name` that is neither a DNS name nor an IP address is refused at
/// configuration time, before a socket is opened.
#[cfg(feature = "tls")]
#[tokio::test]
async fn an_unusable_server_name_is_refused_before_the_dial() {
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    let client_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.tls =
        Some(weida_mqtt::TlsOptions::new(client_config).with_server_name("not a host name"));
    // Port 1 on loopback: nothing is listening, so if the name were accepted
    // this would fail with a connection error instead of a configuration one.
    let error = Client::connect(&context, "127.0.0.1:1", options)
        .await
        .expect_err("refused");
    assert!(matches!(error, Error::Configuration(_)), "{error}");
    assert!(error.to_string().contains("server name"));
}

/// An authenticator for a mechanism that proves the server — SCRAM's shape —
/// and so refuses an exchange that ends without the proof.
pub(crate) struct Mutual {
    pub(crate) proof: &'static [u8],
}

impl Authenticator for Mutual {
    fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>> {
        Ok(vec![1])
    }

    fn finish(&self, data: Option<&[u8]>) -> Result<()> {
        match data {
            Some(data) if data == self.proof => Ok(()),
            _ => Err(Error::AuthenticationFailed(
                "the server did not prove it knows the secret".into(),
            )),
        }
    }
}

pub(crate) fn scram_options() -> ConnectOptions {
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    options
}
