//! The three ways a process authenticates, hand-off included.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{Value, json};

use crate::client::OpenBao;
use crate::{Error, Result};

/// What a login returned about the token it produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenInfo {
    /// The accessor: names the token without being it, so a controller can
    /// revoke at exit what it never held.
    pub accessor: Option<String>,
    /// How long the token lives before it needs renewing; zero for a token
    /// that never expires.
    pub ttl: Duration,
    /// Whether `auth/token/renew-self` is allowed.
    pub renewable: bool,
    /// The policies attached.
    pub policies: Vec<String>,
}

impl TokenInfo {
    /// Reads the `auth` object of a login or renewal answer.
    pub(crate) fn from_auth(answer: &Value) -> Result<TokenInfo> {
        let auth = answer
            .get("auth")
            .filter(|a| !a.is_null())
            .ok_or_else(|| Error::Shape("no `auth` object".into()))?;
        TokenInfo::from_object(auth, "lease_duration")
    }

    /// Reads the token fields of one object: the `auth` object of a login or
    /// renewal answer, whose lifetime is `lease_duration`, or the `data`
    /// object of a lookup, whose lifetime is `ttl`.
    pub(crate) fn from_object(object: &Value, ttl_key: &str) -> Result<TokenInfo> {
        Ok(TokenInfo {
            accessor: object
                .get("accessor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            ttl: Duration::from_secs(object.get(ttl_key).and_then(Value::as_u64).unwrap_or(0)),
            renewable: object
                .get("renewable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            policies: object
                .get("policies")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|p| p.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
        })
    }

    /// The token itself, from the same object.
    fn client_token(answer: &Value) -> Result<String> {
        answer
            .get("auth")
            .and_then(|a| a.get("client_token"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| Error::Shape("no `auth.client_token`".into()))
    }
}

/// Where a hand-off's wrapping token is read from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandoffSource {
    /// A file, typically one systemd's `SetCredential=` or `LoadCredential=`
    /// placed under `$CREDENTIALS_DIRECTORY`.
    File(PathBuf),
    /// `$CREDENTIALS_DIRECTORY/<name>`: the systemd credential by its name.
    Credential(String),
    /// The token itself, already in memory. For a caller that received it
    /// some other way; nothing here reads the environment, because an
    /// environment variable is readable by every process of the uid.
    Token(String),
}

impl HandoffSource {
    fn read(&self) -> Result<String> {
        let path = match self {
            HandoffSource::Token(token) => return Ok(token.trim().to_owned()),
            HandoffSource::File(path) => path.clone(),
            HandoffSource::Credential(name) => {
                let dir = std::env::var_os("CREDENTIALS_DIRECTORY").ok_or_else(|| {
                    Error::Credential(
                        "CREDENTIALS_DIRECTORY is not set: not started with a credential".into(),
                    )
                })?;
                PathBuf::from(dir).join(name)
            }
        };
        let token = std::fs::read_to_string(&path)
            .map_err(|e| Error::Credential(format!("{}: {e}", path.display())))?;
        let token = token.trim();
        if token.is_empty() {
            return Err(Error::Credential(format!("{}: empty", path.display())));
        }
        Ok(token.to_owned())
    }
}

/// How the client proves who it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Auth {
    /// A token the process was given: `BAO_TOKEN`, a dev root, a token the
    /// operator issued. Looked up once so the client knows its TTL.
    Token(String),
    /// AppRole: `auth/approle/login` with a role id and a secret id.
    AppRole {
        /// The role id.
        role_id: String,
        /// The secret id — itself often delivered wrapped.
        secret_id: String,
        /// The mount, `approle` by default.
        mount: String,
    },
    /// The hand-off (0032 §2): a response-wrapping token minted by the
    /// controller around a token it created for this process. Redeemed
    /// **once**, as the client's first request; a second redemption by
    /// anyone is the alarm.
    Handoff(HandoffSource),
}

impl Auth {
    /// AppRole on the default mount.
    pub fn approle(role_id: impl Into<String>, secret_id: impl Into<String>) -> Auth {
        Auth::AppRole {
            role_id: role_id.into(),
            secret_id: secret_id.into(),
            mount: "approle".into(),
        }
    }

    /// The hand-off from a systemd credential by name.
    pub fn handoff_credential(name: impl Into<String>) -> Auth {
        Auth::Handoff(HandoffSource::Credential(name.into()))
    }

    pub(crate) async fn login(self, client: &OpenBao) -> Result<(String, TokenInfo)> {
        match self {
            Auth::Token(token) => {
                let probe = client.clone();
                probe.install(token.clone(), TokenInfo::unknown());
                let answer = probe.get("auth/token/lookup-self").await?;
                let data = answer
                    .get("data")
                    .ok_or_else(|| Error::Shape("no `data` in lookup-self".into()))?;
                Ok((token, TokenInfo::from_object(data, "ttl")?))
            }
            Auth::AppRole {
                role_id,
                secret_id,
                mount,
            } => {
                let answer = client
                    .post_anonymous(
                        &format!("auth/{mount}/login"),
                        json!({ "role_id": role_id, "secret_id": secret_id }),
                    )
                    .await?;
                Ok((
                    TokenInfo::client_token(&answer)?,
                    TokenInfo::from_auth(&answer)?,
                ))
            }
            Auth::Handoff(source) => {
                let wrapping = source.read()?;
                let answer = client.unwrap(&wrapping).await?;
                Ok((
                    TokenInfo::client_token(&answer)?,
                    TokenInfo::from_auth(&answer)?,
                ))
            }
        }
    }
}

impl TokenInfo {
    /// A placeholder while a given token is looked up.
    fn unknown() -> TokenInfo {
        TokenInfo {
            accessor: None,
            ttl: Duration::ZERO,
            renewable: false,
            policies: Vec::new(),
        }
    }
}
