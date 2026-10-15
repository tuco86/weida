//! An identity read from a KV v2 secret.

use std::time::Duration;

use serde_json::Value;
use weida::{Identity, IdentitySource};

use crate::client::OpenBao;
use crate::{Error, Result};

/// Certificate chain and key stored as two fields of one KV v2 secret:
/// distribution and backup of an identity somebody made elsewhere, with no
/// rotation of its own (0032 §2). The secret holds the private key, which
/// is the trade this source is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kv {
    /// The mount, `secret` by default.
    pub mount: String,
    /// The secret's path under the mount.
    pub path: String,
    /// The field holding the PEM chain, `certificate` by default.
    pub cert_field: String,
    /// The field holding the PEM key, `private_key` by default.
    pub key_field: String,
    /// How often the secret is read again; `None` reads it once. A changed
    /// key is a new peer, and the source says so.
    pub refresh: Option<Duration>,
}

impl Kv {
    /// `secret/data/<path>` with the default field names, read once.
    pub fn new(path: impl Into<String>) -> Kv {
        Kv {
            mount: "secret".into(),
            path: path.into(),
            cert_field: "certificate".into(),
            key_field: "private_key".into(),
            refresh: None,
        }
    }

    /// Reads the secret now and, with `refresh` set, keeps the returned
    /// source current until the application drops it.
    pub async fn start(self, client: OpenBao) -> Result<IdentitySource> {
        let identity = self.fetch(&client).await?;
        let source = IdentitySource::external(identity)?;
        if let Some(refresh) = self.refresh {
            let refreshing = source.downgrade();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(refresh).await;
                    // Weak, so that the task — and the client it holds — ends
                    // with the source it serves instead of outliving it.
                    let Some(held) = refreshing.upgrade() else {
                        return;
                    };
                    match self.fetch(&client).await {
                        Ok(identity) => {
                            if identity != held.current() {
                                let _ = held.update(identity);
                            }
                        }
                        Err(e) => held.report_failure(e.to_string()),
                    }
                }
            });
        }
        Ok(source)
    }

    async fn fetch(&self, client: &OpenBao) -> Result<Identity> {
        let mut answer = client
            .get(&format!("{}/data/{}", self.mount, self.path))
            .await?;
        let data = answer
            .get_mut("data")
            .and_then(|d| d.get_mut("data"))
            .ok_or_else(|| Error::Shape("no `data.data` in the KV answer".into()))?;
        // Moved out of the answer, not copied: the key is the one thing in it a
        // copy would leave behind in a tree that is not wiped when dropped.
        let mut field = |name: &str| match data.get_mut(name) {
            Some(Value::String(text)) => Ok(std::mem::take(text).into_bytes()),
            _ => Err(Error::Shape(format!("no `{name}` field in the secret"))),
        };
        let cert = field(&self.cert_field)?;
        // One field holding both halves is a combined PEM, which weida reads
        // for either: the same bytes twice.
        let key = if self.key_field == self.cert_field {
            cert.clone()
        } else {
            field(&self.key_field)?
        };
        Ok(Identity::from_pem(cert, key))
    }
}
