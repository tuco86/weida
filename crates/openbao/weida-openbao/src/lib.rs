//! OpenBao (and Vault) for weida
//! ([decisions/0032](../../../docs/decisions/0032-identity-sources-and-the-handoff.md)).
//!
//! Three things, in the order a process needs them:
//!
//! 1. **A client** ([`OpenBao`]) that authenticates one of three ways
//!    ([`Auth`]): a token it was given, an AppRole login, or a **hand-off** —
//!    a response-wrapping token minted by a controller, redeemed exactly once
//!    and before anything else, whose failure to redeem means somebody else
//!    already did ([`Error::HandoffStolen`], 0032 §2). The client keeps its
//!    token renewed for as long as it lives.
//! 2. **An identity signed by a PKI mount** ([`PkiSign`]): the key stays
//!    local, a CSR over it is signed by a role, and the certificate is
//!    renewed under the **same key** — so weida's fingerprint, and the
//!    redial of [0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)
//!    that pins it, survive every rotation. `pki/issue`, which would rotate
//!    the key, is deliberately not offered.
//! 3. **Trust anchored on the mount's CA** ([`PkiAnchor`]) and **material
//!    read from KV** ([`Kv`]), for the peers and the deployments that need
//!    them.
//!
//! What this crate does not do: check revocation (no CRL, no OCSP; the TTL
//! is the answer, and the renewal is what makes a short one affordable).
//!
//! The controller half of the hand-off, minting the wrapped token and
//! revoking the child by its accessor, is [`OpenBao::post_wrapped`] and
//! [`OpenBao::revoke_accessor`], driven from the unit file in
//! `examples/handoff`.

#![forbid(unsafe_code)]

mod auth;
mod client;
mod kv;
mod pki;

pub use auth::{Auth, HandoffSource, TokenInfo};
pub use client::{Config, OpenBao};
pub use kv::Kv;
pub use pki::{PkiAnchor, PkiSign};

/// Everything that can go wrong talking to OpenBao.
#[derive(Debug)]
pub enum Error {
    /// The request could not be made or answered at the HTTP level.
    Http(String),
    /// OpenBao answered with an error status and, usually, a list of
    /// reasons.
    Api {
        /// The HTTP status.
        status: u16,
        /// The `errors` array of the response, or the raw body.
        errors: Vec<String>,
    },
    /// The hand-off's wrapping token had already been redeemed — or never
    /// existed, which from this side is the same thing — before this process
    /// could redeem it (0032 §2). The process should not continue.
    HandoffStolen,
    /// An operation needed a token and the client has none.
    Unauthenticated,
    /// The hand-off credential could not be read from where it was said to
    /// be.
    Credential(String),
    /// The answer had a shape this crate does not understand.
    Shape(String),
    /// TLS material for the connection to OpenBao could not be used.
    Tls(String),
    /// weida refused the material this crate produced.
    Weida(weida::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Http(m) => write!(f, "openbao request failed: {m}"),
            Error::Api { status, errors } => {
                write!(f, "openbao answered {status}: {}", errors.join("; "))
            }
            Error::HandoffStolen => f.write_str(
                "the hand-off token was already redeemed: somebody else read this process's \
                 credential first",
            ),
            Error::Unauthenticated => f.write_str("no openbao token: log in first"),
            Error::Credential(m) => write!(f, "hand-off credential unreadable: {m}"),
            Error::Shape(m) => write!(f, "unexpected openbao response: {m}"),
            Error::Tls(m) => write!(f, "openbao tls: {m}"),
            Error::Weida(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<weida::Error> for Error {
    fn from(e: weida::Error) -> Error {
        Error::Weida(e)
    }
}

/// The result type of this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;
