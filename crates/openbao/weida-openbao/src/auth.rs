//! The three ways a process authenticates, hand-off included.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{Value, json};
use zeroize::Zeroizing;

use crate::client::OpenBao;
use crate::secret::Secret;
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

    /// The token itself, **moved out** of the answer: a copy would leave the
    /// original behind in a tree that is not wiped when it is dropped.
    fn take_client_token(answer: &mut Value) -> Result<Secret> {
        match answer
            .get_mut("auth")
            .and_then(|a| a.get_mut("client_token"))
        {
            Some(Value::String(token)) => Ok(Secret::from(std::mem::take(token))),
            _ => Err(Error::Shape("no `auth.client_token`".into())),
        }
    }
}

/// Where a hand-off's wrapping token is read from.
///
/// Its `Debug` hides a token held in memory, and it is not comparable: see
/// [`Secret`].
#[derive(Clone)]
pub enum HandoffSource {
    /// A file, typically one systemd's `SetCredential=` or `LoadCredential=`
    /// placed under `$CREDENTIALS_DIRECTORY`.
    File(PathBuf),
    /// `$CREDENTIALS_DIRECTORY/<name>`: the systemd credential by its name.
    Credential(String),
    /// The token itself, already in memory. For a caller that received it
    /// some other way; nothing here reads the environment, because an
    /// environment variable is readable by every process of the uid.
    Token(Secret),
}

impl std::fmt::Debug for HandoffSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandoffSource::File(path) => f.debug_tuple("File").field(path).finish(),
            HandoffSource::Credential(name) => f.debug_tuple("Credential").field(name).finish(),
            HandoffSource::Token(_) => f.write_str("Token(<redacted>)"),
        }
    }
}

impl HandoffSource {
    fn read(&self) -> Result<Secret> {
        let path = match self {
            HandoffSource::Token(token) => return Ok(Secret::from(token.expose().trim())),
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
            .map(Zeroizing::new)
            .map_err(|e| Error::Credential(format!("{}: {e}", path.display())))?;
        let token = token.trim();
        if token.is_empty() {
            return Err(Error::Credential(format!("{}: empty", path.display())));
        }
        Ok(Secret::from(token))
    }
}

/// How the client proves who it is.
///
/// Its `Debug` hides every credential and it is not comparable: a secret that
/// can be printed or `==`-compared ends up in a log or behind a timing
/// difference. See [`Secret`].
#[derive(Clone)]
pub enum Auth {
    /// A token the process was given: `BAO_TOKEN`, a dev root, a token the
    /// operator issued. Looked up once so the client knows its TTL.
    Token(Secret),
    /// AppRole: `auth/approle/login` with a role id and a secret id.
    AppRole {
        /// The role id.
        role_id: String,
        /// The secret id — itself often delivered wrapped.
        secret_id: Secret,
        /// The mount, `approle` by default.
        mount: String,
    },
    /// The hand-off (0032 §2): a response-wrapping token minted by the
    /// controller around a token it created for this process. Redeemed
    /// **once**, as the client's first request; a second redemption by
    /// anyone is the alarm.
    Handoff(HandoffSource),
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::Token(_) => f.write_str("Token(<redacted>)"),
            Auth::AppRole { role_id, mount, .. } => f
                .debug_struct("AppRole")
                .field("role_id", role_id)
                .field("secret_id", &"<redacted>")
                .field("mount", mount)
                .finish(),
            Auth::Handoff(source) => f.debug_tuple("Handoff").field(source).finish(),
        }
    }
}

impl Auth {
    /// AppRole on the default mount.
    pub fn approle(role_id: impl Into<String>, secret_id: impl Into<Secret>) -> Auth {
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

    pub(crate) async fn login(self, client: &OpenBao) -> Result<(Secret, TokenInfo)> {
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
                let mut answer = client
                    .post_anonymous(
                        &format!("auth/{mount}/login"),
                        json!({ "role_id": role_id, "secret_id": secret_id.expose() }),
                    )
                    .await?;
                let info = TokenInfo::from_auth(&answer)?;
                Ok((TokenInfo::take_client_token(&mut answer)?, info))
            }
            Auth::Handoff(source) => {
                let wrapping = source.read()?;
                let mut answer = client.unwrap(wrapping.expose()).await?;
                let info = TokenInfo::from_auth(&answer)?;
                Ok((TokenInfo::take_client_token(&mut answer)?, info))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_credential_survives_a_debug_print() {
        let approle = Auth::approle("role-1", "approle-secret-id");
        let token = Auth::Token("tok-s3cret".into());
        let source = HandoffSource::Token("wrap-s3cret".into());
        let handoff = Auth::Handoff(source.clone());
        for printed in [
            format!("{approle:?}"),
            format!("{approle:#?}"),
            format!("{token:?}"),
            format!("{source:?}"),
            format!("{handoff:#?}"),
        ] {
            assert!(
                !printed.contains("approle-secret-id") && !printed.contains("s3cret"),
                "{printed}"
            );
        }
        // What is not a secret is still said: a log line has to name the role.
        assert!(format!("{approle:?}").contains("role-1"));
    }
}
