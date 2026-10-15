//! `**options`: every `CONNECT` field and every bound, read from keywords.
//!
//! One function reads them, and both surfaces call it, so `connect` and
//! `sync.connect` cannot come to differ about what an option is called or
//! what it defaults to. The defaults are not written here at all: they are
//! [`ConnectionOptions::new`]'s, which is where the library states for each
//! bound whether it is the protocol's or its own.
//!
//! # An unknown keyword is refused
//!
//! `connect("127.0.0.1", 4222, ping_intervall=5)` raises
//! `weida_nats.Configuration` naming every keyword there is. A `**options`
//! that silently ignores what it does not recognise is a configuration
//! surface where a typo means "the default, forever" — and the library's own
//! rule is to refuse an unusable configuration where it was configured
//! rather than correct it silently
//! (`docs/decisions/0013-competitor-libraries.md` §4.4 item 4).
//!
//! # The credential forms
//!
//! The reference's four forms are alternatives, and the library makes that
//! structural with an enum, so this reads at most one of them:
//!
//! | Keywords | `Credentials` |
//! | --- | --- |
//! | `token=` | `Token` |
//! | `user=`, `password=` | `UserPassword` |
//! | `jwt=`, optionally `signer=` | `Jwt` |
//! | `nkey_signer=` | `Nkey` |
//!
//! `signer` and `nkey_signer` are Python callables, because the signing is
//! the caller's: an NKey signature is Ed25519 over the server's nonce with
//! the user's seed, and a messaging library that held a private key and chose
//! an algorithm would be making the application's security decision for it.
//! The callable receives the nonce as `bytes`, unchanged, and returns either
//! the signature as `str` or a `(signature, public_key)` pair — the pair
//! being what an NKey `CONNECT` needs, since it carries the public key the
//! server verifies against.

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use weida_nats::options::{Credentials, NonceSignature, Signer};
use weida_nats::{ConnectionOptions, Error};

use crate::errors::raise;

/// Every keyword this module accepts, for the message an unknown one gets.
const KEYWORDS: &str = "name, token, user, password, jwt, signer, nkey_signer, verbose, \
                        pedantic, headers, no_responders, echo, ping_interval, max_pings_out, \
                        inbox_prefix, max_subscriptions, max_pending_requests, \
                        subscription_queue, max_queued_bytes, outgoing_queue, \
                        max_connect_urls, max_header_entries, max_control_line, \
                        max_inbound_control_line, max_info_line, max_inbound_payload, \
                        handshake_timeout, \
                        write_timeout, max_resolved_addresses";

/// Reads `**options` into the library's configuration.
///
/// The credential keywords are collected first and folded into the one
/// [`Credentials`] value at the end, so `token=` beside `user=` is refused as
/// the contradiction it is rather than resolved by keyword order.
pub fn from_kwargs(
    py: Python<'_>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<ConnectionOptions> {
    let mut options = ConnectionOptions::new();
    let mut token: Option<String> = None;
    let mut user: Option<String> = None;
    let mut password: Option<String> = None;
    let mut jwt: Option<String> = None;
    let mut signer: Option<Signer> = None;
    let mut nkey_signer: Option<Signer> = None;

    if let Some(kwargs) = kwargs {
        for (key, value) in kwargs.iter() {
            let key = key.extract::<String>()?;
            match key.as_str() {
                "name" => options.name = Some(value.extract()?),
                "token" => token = Some(value.extract()?),
                "user" => user = Some(value.extract()?),
                "password" => password = Some(value.extract()?),
                "jwt" => jwt = Some(value.extract()?),
                "signer" => signer = Some(signer_of(&value)?),
                "nkey_signer" => nkey_signer = Some(signer_of(&value)?),
                "verbose" => options.verbose = value.extract()?,
                "pedantic" => options.pedantic = value.extract()?,
                "headers" => options.headers = value.extract()?,
                "no_responders" => options.no_responders = value.extract()?,
                "echo" => options.echo = Some(value.extract()?),
                "ping_interval" => options.ping_interval = seconds(py, "ping_interval", &value)?,
                "max_pings_out" => options.max_pings_out = value.extract()?,
                "inbox_prefix" => options.inbox_prefix = value.extract()?,
                "max_subscriptions" => options.max_subscriptions = value.extract()?,
                "max_pending_requests" => options.max_pending_requests = value.extract()?,
                "subscription_queue" => options.subscription_queue = value.extract()?,
                "max_queued_bytes" => options.max_queued_bytes = value.extract()?,
                "outgoing_queue" => options.outgoing_queue = value.extract()?,
                "max_connect_urls" => options.max_connect_urls = value.extract()?,
                "max_header_entries" => options.max_header_entries = value.extract()?,
                "max_control_line" => options.max_control_line = value.extract()?,
                "max_inbound_control_line" => {
                    options.max_inbound_control_line = value.extract()?;
                }
                "max_info_line" => options.max_info_line = value.extract()?,
                "max_inbound_payload" => options.max_inbound_payload = value.extract()?,
                "handshake_timeout" => {
                    options.handshake_timeout = seconds(py, "handshake_timeout", &value)?;
                }
                "write_timeout" => {
                    options.write_timeout = seconds(py, "write_timeout", &value)?;
                }
                "max_resolved_addresses" => options.max_resolved_addresses = value.extract()?,
                unknown => {
                    return Err(configuration(
                        py,
                        format!(
                            "{unknown} is not a connection option. The options are: {KEYWORDS}"
                        ),
                    ));
                }
            }
        }
    }

    options.credentials = credentials(py, token, user, password, jwt, signer, nkey_signer)?;
    // Refused here rather than at the first write: `Connection::connect`
    // validates too, and asking twice costs nothing while letting
    // `sync.connect` fail in the caller's own thread.
    raise(py, options.validate())?;
    Ok(options)
}

/// Folds the credential keywords into the one form a `CONNECT` carries.
fn credentials(
    py: Python<'_>,
    token: Option<String>,
    user: Option<String>,
    password: Option<String>,
    jwt: Option<String>,
    signer: Option<Signer>,
    nkey_signer: Option<Signer>,
) -> PyResult<Credentials> {
    let named = [
        token.is_some(),
        user.is_some() || password.is_some(),
        jwt.is_some(),
        nkey_signer.is_some(),
    ];
    if named.iter().filter(|given| **given).count() > 1 {
        return Err(configuration(
            py,
            "a CONNECT carries one credential form: give token, or user and password, \
             or jwt, or nkey_signer — not several",
        ));
    }
    if let Some(token) = token {
        return Ok(Credentials::Token(token));
    }
    if user.is_some() || password.is_some() {
        let (Some(user), Some(password)) = (user, password) else {
            return Err(configuration(
                py,
                "user and password go together: CONNECT.user without CONNECT.pass is a \
                 credential the server cannot check",
            ));
        };
        return Ok(Credentials::UserPassword { user, password });
    }
    if let Some(jwt) = jwt {
        return Ok(Credentials::Jwt { jwt, signer });
    }
    if let Some(nkey_signer) = nkey_signer {
        return Ok(Credentials::Nkey(nkey_signer));
    }
    if signer.is_some() {
        return Err(configuration(
            py,
            "signer signs the nonce for a jwt credential; give jwt as well, or use \
             nkey_signer for the NKey form",
        ));
    }
    Ok(Credentials::None)
}

/// Wraps a Python callable as the library's nonce signer.
///
/// Called on a reactor thread during the handshake, with no GIL held, so it
/// takes one for the call and gives it back. A refusal — an exception, or an
/// answer of the wrong shape — becomes `Error::Signature`, whose words are
/// the application's because the signing is.
fn signer_of(callable: &Bound<'_, PyAny>) -> PyResult<Signer> {
    if !callable.is_callable() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "a signer is called with the server's nonce as bytes, so it must be callable",
        ));
    }
    let callable: Py<PyAny> = callable.clone().unbind();
    Ok(Signer::new(move |nonce: &[u8]| {
        Python::attach(|py| {
            let answer = callable
                .call1(py, (PyBytes::new(py, nonce),))
                .map_err(|error| format!("the signer raised {error}"))?;
            let answer = answer.bind(py);
            // A bare string is the JWT form's answer; a pair is the NKey
            // form's, because that `CONNECT` carries the public key the
            // server verifies the signature against.
            if let Ok((signature, public_key)) = answer.extract::<(String, String)>() {
                return Ok(NonceSignature {
                    signature,
                    public_key: Some(public_key),
                });
            }
            match answer.extract::<String>() {
                Ok(signature) => Ok(NonceSignature {
                    signature,
                    public_key: None,
                }),
                Err(_) => Err(format!(
                    "a signer returns the base64url signature as str, or a \
                     (signature, public_key) pair; it returned {answer:?}"
                )),
            }
        })
    }))
}

/// A duration in seconds, refused rather than rounded where it is not one.
pub fn seconds(py: Python<'_>, what: &str, value: &Bound<'_, PyAny>) -> PyResult<Duration> {
    let value = value.extract::<f64>()?;
    if !value.is_finite() || value < 0.0 {
        return Err(configuration(
            py,
            format!("{what} is a finite, non-negative number of seconds, not {value}"),
        ));
    }
    Ok(Duration::from_secs_f64(value))
}

/// A duration argument that is already an `f64`, for the call sites where the
/// window is a positional parameter rather than a keyword.
pub fn window(py: Python<'_>, what: &str, value: f64) -> PyResult<Duration> {
    if !value.is_finite() || value < 0.0 {
        return Err(configuration(
            py,
            format!("{what} is a finite, non-negative number of seconds, not {value}"),
        ));
    }
    Ok(Duration::from_secs_f64(value))
}

/// A configuration this binding refuses, in the library's own vocabulary so
/// that `except weida_nats.Configuration` catches both its refusals and ours.
fn configuration(py: Python<'_>, why: impl Into<String>) -> PyErr {
    crate::errors::to_py(
        py,
        &crate::errors::errno_of(Error::Configuration(why.into())),
    )
}
