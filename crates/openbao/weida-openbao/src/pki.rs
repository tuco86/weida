//! Identity signed by a PKI mount, and trust anchored on its CA.

use std::net::IpAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use weida::{
    Identity, IdentitySource, Pem, Trust, TrustSource, WeakIdentitySource, WeakTrustSource,
};

use crate::client::OpenBao;
use crate::{Error, Result};

/// An identity whose certificate a PKI role signs, over a key that stays
/// where it is (0032 §4.3).
///
/// The key comes from the [`IdentitySource`] handed to [`PkiSign::start`] —
/// a `files` source, typically — and never changes: every renewal is a CSR
/// over the same key, so the fingerprint peers pin, and the redial that
/// pins it, are unaffected. What the role must allow is in 0032 §2:
/// `key_type`/`key_bits` matching the key, `allowed_domains` and
/// `allow_ip_sans` for the names, the `server_flag`/`client_flag` the
/// peer's verifier expects, `use_csr_sans`, and a `max_ttl` at or above
/// `ttl`.
#[derive(Clone, Debug, PartialEq)]
pub struct PkiSign {
    /// The mount, `pki` by default.
    pub mount: String,
    /// The role under the mount.
    pub role: String,
    /// DNS names for the certificate. The first is also the common name,
    /// for a role that still requires one.
    pub names: Vec<String>,
    /// IP SANs, for a peer that is dialled by address.
    pub ips: Vec<IpAddr>,
    /// The TTL asked for; `None` takes the role's default.
    pub ttl: Option<Duration>,
    /// When to renew, as a fraction of the certificate's lifetime elapsed.
    /// Two thirds by default: early enough that a failed renewal is reported
    /// and retried while the certificate still verifies.
    pub renew_at: f64,
}

impl PkiSign {
    /// A role under the `pki` mount, for `names`.
    pub fn new(
        role: impl Into<String>,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> PkiSign {
        PkiSign {
            mount: "pki".into(),
            role: role.into(),
            names: names.into_iter().map(Into::into).collect(),
            ips: Vec::new(),
            ttl: None,
            renew_at: 2.0 / 3.0,
        }
    }

    /// Signs the key of `key_source` now, and keeps the returned source
    /// renewed until the application drops it.
    ///
    /// The returned source starts with the signed chain over that key. A
    /// renewal that fails is reported on its event stream as
    /// `RenewalFailed` and retried; the certificate in service is unchanged
    /// until a renewal succeeds. `key_source` is read for its key only; the
    /// certificate it holds — self-signed, typically — is never served.
    pub async fn start(
        self,
        client: OpenBao,
        key_source: &IdentitySource,
    ) -> Result<IdentitySource> {
        // Local configuration, checked where it enters: a fraction that is not
        // a number would otherwise reach the renewal task, which is the one
        // place that must not fail.
        if !self.renew_at.is_finite() {
            return Err(Error::Config(format!(
                "renew_at {} is not a fraction of the certificate's lifetime",
                self.renew_at
            )));
        }
        let key = key_source.current().key;
        let (identity, expires) = self.sign(&client, &key).await?;
        let source = IdentitySource::external(identity)?;
        let renewing = source.downgrade();
        tokio::spawn(async move { self.renew_forever(client, key, renewing, expires).await });
        Ok(source)
    }

    /// One signing round: CSR over `key`, `pki/sign/<role>`, the chain.
    async fn sign(&self, client: &OpenBao, key: &Pem) -> Result<(Identity, SystemTime)> {
        let csr = csr_over(key, &self.names, &self.ips)?;
        let mut body = json!({
            "csr": csr,
            "use_csr_sans": true,
            "format": "pem",
        });
        if let Some(first) = self.names.first() {
            body["common_name"] = Value::String(first.clone());
        }
        if let Some(ttl) = self.ttl {
            body["ttl"] = Value::String(format!("{}s", ttl.as_secs()));
        }
        let answer = client
            .post(&format!("{}/sign/{}", self.mount, self.role), body)
            .await?;
        let data = answer
            .get("data")
            .ok_or_else(|| Error::Shape("no `data` in the sign answer".into()))?;
        let certificate = data
            .get("certificate")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Shape("no `data.certificate`".into()))?;
        let mut chain = String::from(certificate.trim_end());
        chain.push('\n');
        if let Some(cas) = data.get("ca_chain").and_then(Value::as_array) {
            for ca in cas.iter().filter_map(Value::as_str) {
                chain.push_str(ca.trim_end());
                chain.push('\n');
            }
        } else if let Some(ca) = data.get("issuing_ca").and_then(Value::as_str) {
            chain.push_str(ca.trim_end());
            chain.push('\n');
        }
        // The server's number, so checked: a date past what `SystemTime` can
        // hold would panic in the addition, and the task that renews the
        // certificate must not be the one that dies of it.
        let expires = data
            .get("expiration")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::Shape("no `data.expiration`".into()))
            .and_then(|secs| {
                UNIX_EPOCH
                    .checked_add(Duration::from_secs(secs))
                    .ok_or_else(|| Error::Shape("`data.expiration` is out of range".into()))
            })?;
        let identity = Identity {
            cert_chain: Pem::Bytes(chain.into_bytes()),
            key: key.clone(),
        };
        Ok((identity, expires))
    }

    /// Renews until the application drops the source it was given.
    ///
    /// The task holds the source weakly: a strong handle in a task that never
    /// ends would be a source that never ends, and with it this client and the
    /// key. It holds one only while a round is under way, and returns — and
    /// drops the client and the key with it — the first time it wakes to find
    /// nobody else does.
    async fn renew_forever(
        self,
        client: OpenBao,
        key: Pem,
        source: WeakIdentitySource,
        mut expires: SystemTime,
    ) {
        let mut issued = SystemTime::now();
        loop {
            let lifetime = expires
                .duration_since(issued)
                .unwrap_or(Duration::from_secs(60));
            let renew_after = lifetime.mul_f64(self.renew_at.clamp(0.1, 0.95));
            let wait = match issued.checked_add(renew_after) {
                Some(due) => due
                    .duration_since(SystemTime::now())
                    .unwrap_or(Duration::ZERO),
                // A certificate valid for longer than a clock can count: wait
                // as long as the arithmetic says and no longer.
                None => renew_after,
            }
            .max(Duration::from_secs(1));
            tokio::time::sleep(wait).await;
            let Some(held) = source.upgrade() else {
                return;
            };
            let retry = match self.sign(&client, &key).await {
                Ok((identity, next)) => match held.update(identity) {
                    Ok(()) => {
                        issued = SystemTime::now();
                        expires = next;
                        None
                    }
                    // The source reported it; retry after a fraction of what
                    // is left.
                    Err(_) => Some(retry_after(expires)),
                },
                Err(e) => {
                    held.report_failure(e.to_string());
                    Some(retry_after(expires))
                }
            };
            drop(held);
            if let Some(retry) = retry {
                tokio::time::sleep(retry).await;
            }
        }
    }
}

/// How long to wait before retrying a failed renewal: a tenth of what is
/// left of the certificate, between one second and a minute.
fn retry_after(expires: SystemTime) -> Duration {
    let left = expires
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO);
    (left / 10).clamp(Duration::from_secs(1), Duration::from_secs(60))
}

/// A PEM certificate signing request over `key`, naming `names` and `ips`.
fn csr_over(key: &Pem, names: &[String], ips: &[IpAddr]) -> Result<String> {
    // Borrowed when the key is in memory and wiped when it was read from a
    // file: this function must not leave a second copy of a private key in
    // memory for the allocator to hand to somebody else.
    let read;
    let key_pem: &str = match key {
        Pem::Bytes(bytes) => {
            std::str::from_utf8(bytes).map_err(|_| Error::Tls("the key is not UTF-8 PEM".into()))?
        }
        Pem::File(path) => {
            read = zeroize::Zeroizing::new(
                std::fs::read_to_string(path)
                    .map_err(|e| Error::Tls(format!("reading the key {}: {e}", path.display())))?,
            );
            &read
        }
    };
    let key_pair = rcgen::KeyPair::from_pem(key_pem)
        .map_err(|e| Error::Tls(format!("parsing the key for the CSR: {e}")))?;
    let mut sans: Vec<rcgen::SanType> = names
        .iter()
        .map(|n| {
            rcgen::string::Ia5String::try_from(n.as_str())
                .map(rcgen::SanType::DnsName)
                .map_err(|e| Error::Tls(format!("name {n:?}: {e}")))
        })
        .collect::<Result<_>>()?;
    sans.extend(ips.iter().map(|ip| rcgen::SanType::IpAddress(*ip)));
    let mut params = rcgen::CertificateParams::default();
    params.subject_alt_names = sans;
    if let Some(first) = names.first() {
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, first.clone());
    }
    let csr = params
        .serialize_request(&key_pair)
        .map_err(|e| Error::Tls(format!("building the CSR: {e}")))?;
    csr.pem()
        .map_err(|e| Error::Tls(format!("encoding the CSR: {e}")))
}

/// Trust anchored on a PKI mount's CA, refreshed at an interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkiAnchor {
    /// The mount, `pki` by default.
    pub mount: String,
    /// How often the CA is fetched again. A rotated CA reaches the next
    /// handshake after at most this long.
    pub refresh: Duration,
    /// Fingerprints trusted outright beside the anchor.
    pub pins: Vec<weida::Fingerprint>,
}

impl PkiAnchor {
    /// The `pki` mount, refreshed hourly.
    pub fn new() -> PkiAnchor {
        PkiAnchor {
            mount: "pki".into(),
            refresh: Duration::from_secs(3600),
            pins: Vec::new(),
        }
    }

    /// Fetches the CA chain now and keeps the returned source refreshed until
    /// the application drops it.
    pub async fn start(self, client: OpenBao) -> Result<TrustSource> {
        let trust = self.fetch(&client).await?;
        let source = TrustSource::external(trust.clone());
        let refreshing: WeakTrustSource = source.downgrade();
        tokio::spawn(async move {
            let mut last = trust;
            loop {
                tokio::time::sleep(self.refresh).await;
                // Weak, so that the task ends with the source it serves.
                let Some(held) = refreshing.upgrade() else {
                    return;
                };
                match self.fetch(&client).await {
                    Ok(trust) => {
                        if last != trust {
                            held.update(trust.clone());
                            last = trust;
                        }
                    }
                    Err(e) => held.report_failure(e.to_string()),
                }
            }
        });
        Ok(source)
    }

    /// `pki/cert/ca_chain`, falling back to `pki/cert/ca` on a mount that
    /// has no chain.
    async fn fetch(&self, client: &OpenBao) -> Result<Trust> {
        let answer = match client.get(&format!("{}/cert/ca_chain", self.mount)).await {
            Ok(answer) => answer,
            Err(Error::Api { status: 404, .. }) => {
                client.get(&format!("{}/cert/ca", self.mount)).await?
            }
            Err(e) => return Err(e),
        };
        let pem = answer
            .get("data")
            .and_then(|d| d.get("certificate"))
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Shape("no `data.certificate` for the CA".into()))?;
        let mut trust = Trust::anchor(pem.as_bytes().to_vec());
        for pin in &self.pins {
            trust = trust.and_pin(*pin);
        }
        Ok(trust)
    }
}

impl Default for PkiAnchor {
    fn default() -> Self {
        PkiAnchor::new()
    }
}
