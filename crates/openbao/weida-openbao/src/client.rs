//! The HTTP client: one base address, one token, JSON in and out.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use serde_json::Value;
use weida::Pem;
use weida_http::{BodyReader, ClientOptions, Fields, Method, Protocol, Scheme, Url};
use weida_runtime::Exec;

use crate::auth::{Auth, TokenInfo};
use crate::secret::Secret;
use crate::{Error, Result};

/// The most bytes of one answer the client reads.
///
/// A deadline bounds a hostile body by bandwidth times time, which is
/// gigabytes; this bounds it by size. The largest thing the client asks for is
/// a KV secret, which OpenBao itself limits to a megabyte, so the cap is far
/// above any real answer and far below what a server — or whatever sits in
/// front of one — can make this process buffer.
pub(crate) const MAX_RESPONSE_BYTES: usize = 16 << 20;

/// The most of a server's text kept in an [`Error::Api`] entry, in bytes.
///
/// A body that is not the JSON an OpenBao answers with is copied into the
/// error and from there into logs; what the other end chooses to write must
/// not be able to flood either.
const MAX_ERROR_TEXT: usize = 1024;

/// The most entries kept from an answer's `errors` array.
const MAX_ERRORS: usize = 16;

/// How to reach OpenBao.
#[derive(Clone, Debug)]
pub struct Config {
    /// `https://bao.example:8200`, or `http://…` for a dev server when
    /// [`Config::allow_insecure`] says so. The value of `BAO_ADDR`.
    pub address: String,
    /// The CA that issued OpenBao's own certificate (`BAO_CACERT`). `None`
    /// trusts the public roots, which is right for a public certificate and
    /// wrong for an internal CA.
    pub ca: Option<Pem>,
    /// An enterprise-style namespace, sent as `X-Vault-Namespace`. Rarely
    /// set on OpenBao.
    pub namespace: Option<String>,
    /// Per-request timeout, answer included.
    pub timeout: Duration,
    /// Allows an `http://` address. Default `false`.
    ///
    /// Over `http://` the token and every secret travel in clear, and whoever
    /// is on the path can read them or answer in the server's place, so the
    /// client refuses to be built for one. Set it for a dev server on this
    /// host, where there is no path.
    pub allow_insecure: bool,
}

impl Config {
    /// Reaches `address` with the public roots and a 10 s timeout, over
    /// `https://` only.
    pub fn new(address: impl Into<String>) -> Config {
        Config {
            address: address.into(),
            ca: None,
            namespace: None,
            timeout: Duration::from_secs(10),
            allow_insecure: false,
        }
    }

    /// From the environment the `bao` CLI reads: `BAO_ADDR` (falling back to
    /// `VAULT_ADDR`), `BAO_CACERT`, `BAO_NAMESPACE`.
    ///
    /// An `http://` address is still refused until
    /// [`Config::allow_insecure`] is set: the environment is not where that
    /// decision is made.
    pub fn from_env() -> Result<Config> {
        let address = std::env::var("BAO_ADDR")
            .or_else(|_| std::env::var("VAULT_ADDR"))
            .map_err(|_| Error::Http("neither BAO_ADDR nor VAULT_ADDR is set".into()))?;
        let mut config = Config::new(address);
        if let Ok(path) = std::env::var("BAO_CACERT").or_else(|_| std::env::var("VAULT_CACERT")) {
            config.ca = Some(Pem::File(path.into()));
        }
        if let Ok(namespace) =
            std::env::var("BAO_NAMESPACE").or_else(|_| std::env::var("VAULT_NAMESPACE"))
        {
            config.namespace = Some(namespace);
        }
        Ok(config)
    }
}

/// What the client holds once logged in.
struct Session {
    token: Secret,
    info: TokenInfo,
    /// When `info` was learned, which is when its TTL starts counting down.
    since: Instant,
}

impl Session {
    fn new(token: Secret, info: TokenInfo) -> Session {
        Session {
            token,
            info,
            since: Instant::now(),
        }
    }
}

/// An OpenBao client: a base address and the token of the moment.
///
/// Cloning shares the token, so the renewal task and every source built on
/// the client see the same session.
///
/// **It follows no redirects.** A redirect is an answer the server's own
/// front end gives, and following one re-sends the request — the token
/// included, and for a `307` the body, which may be a secret id — to whatever
/// host it names. A standby node's redirect to the active node therefore
/// surfaces as an [`Error::Api`] naming the `Location`; address the active
/// node, or let the standby forward.
#[derive(Clone)]
pub struct OpenBao {
    http: weida_http::Client,
    exec: Exec,
    timeout: Duration,
    base: String,
    namespace: Option<String>,
    session: Arc<RwLock<Option<Session>>>,
}

impl OpenBao {
    /// A client for `config`, not yet authenticated.
    ///
    /// Must be created inside a Tokio runtime: the HTTP client runs on the
    /// runtime it was created in.
    ///
    /// Refused with [`Error::Config`] for an address that is not `https://`
    /// unless [`Config::allow_insecure`] allows `http://`.
    pub fn new(config: Config) -> Result<OpenBao> {
        let address = Url::parse(&config.address)
            .map_err(|e| Error::Config(format!("address {:?}: {e}", config.address)))?;
        match address.scheme {
            Scheme::Https => {}
            Scheme::Http if config.allow_insecure => {}
            Scheme::Http => {
                return Err(Error::Config(format!(
                    "address {:?}: http:// sends the token and every secret in clear; use \
                     https://, or set `Config::allow_insecure` for a server on this host",
                    config.address
                )));
            }
        }
        let exec = Exec::current().map_err(|e| Error::Http(e.to_string()))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        match &config.ca {
            Some(pem) => {
                use rustls_pki_types::pem::PemObject as _;
                let certs: Vec<rustls_pki_types::CertificateDer<'static>> = match pem {
                    Pem::Bytes(bytes) => rustls_pki_types::CertificateDer::pem_slice_iter(bytes)
                        .collect::<std::result::Result<_, _>>()
                        .map_err(|e| Error::Tls(format!("parsing the CA: {e}")))?,
                    Pem::File(path) => rustls_pki_types::CertificateDer::pem_file_iter(path)
                        .map_err(|e| Error::Tls(format!("reading the CA: {e}")))?
                        .collect::<std::result::Result<_, _>>()
                        .map_err(|e| Error::Tls(format!("parsing the CA: {e}")))?,
                };
                for cert in certs {
                    roots
                        .add(cert)
                        .map_err(|e| Error::Tls(format!("adding the CA: {e}")))?;
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| Error::Tls(e.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let options = ClientOptions {
            tls: Some(Arc::new(tls)),
            protocol: Protocol::Auto,
            ..ClientOptions::default()
        };
        let http =
            weida_http::Client::new(&exec, options).map_err(|e| Error::Http(e.to_string()))?;
        Ok(OpenBao {
            http,
            exec,
            timeout: config.timeout,
            base: config.address.trim_end_matches('/').to_owned(),
            namespace: config.namespace,
            session: Arc::new(RwLock::new(None)),
        })
    }

    /// Authenticates, replacing any previous session, and keeps the token
    /// renewed for as long as this client — or any clone — lives.
    ///
    /// For [`Auth::Handoff`] this is the redemption: it runs before any
    /// other request this client makes, and its failure is
    /// [`Error::HandoffStolen`], on which the caller should exit (0032 §2).
    pub async fn login(&self, auth: Auth) -> Result<TokenInfo> {
        let (token, info) = auth.login(self).await?;
        *self.session.write().expect("session poisoned") = Some(Session::new(token, info.clone()));
        if info.renewable {
            let client = self.clone();
            tokio::spawn(async move { client.renew_forever().await });
        }
        Ok(info)
    }

    /// The token accessor of the current session, for a controller that
    /// revokes by accessor at process exit.
    pub fn accessor(&self) -> Option<String> {
        self.session
            .read()
            .expect("session poisoned")
            .as_ref()
            .and_then(|s| s.info.accessor.clone())
    }

    /// Sets the session without starting a renewal: for a login that needs
    /// a token to ask about itself.
    pub(crate) fn install(&self, token: Secret, info: TokenInfo) {
        *self.session.write().expect("session poisoned") = Some(Session::new(token, info));
    }

    /// What the current session knows about its token.
    pub fn token_info(&self) -> Option<TokenInfo> {
        self.session
            .read()
            .expect("session poisoned")
            .as_ref()
            .map(|s| s.info.clone())
    }

    /// The session token, for the `X-Vault-Token` field it is sent in.
    fn token(&self) -> Result<String> {
        let session = self.session.read().expect("session poisoned");
        let session = session.as_ref().ok_or(Error::Unauthenticated)?;
        Ok(session.token.expose().to_owned())
    }

    /// What the renewal loop needs to know about the session.
    fn session_clock(&self) -> Option<(TokenInfo, Instant)> {
        self.session
            .read()
            .expect("session poisoned")
            .as_ref()
            .map(|s| (s.info.clone(), s.since))
    }

    /// Renews the session token at half its TTL until the client is gone.
    ///
    /// A renewal that fails is retried on a short backoff measured against
    /// what is left of the token, not after another half of its life: the
    /// second attempt of a token renewed at half-TTL would otherwise come
    /// after the token had expired. A renewal that reports a shorter TTL is
    /// followed at that pace; a token that stops being renewable, or that
    /// never expires (a TTL of zero), ends the loop, and so does an answer
    /// that says the token is no longer valid.
    async fn renew_forever(self) {
        let mut failures: u32 = 0;
        loop {
            let Some((info, since)) = self.session_clock() else {
                return;
            };
            if !info.renewable || info.ttl.is_zero() {
                return;
            }
            tokio::time::sleep(renewal_wait(info.ttl, since.elapsed(), failures)).await;
            if Arc::strong_count(&self.session) == 1 {
                // Only this task holds the session: the client is gone.
                return;
            }
            match self.renew_self().await {
                Ok(info) => {
                    failures = 0;
                    if let Some(session) = self.session.write().expect("session poisoned").as_mut()
                    {
                        session.info = info;
                        session.since = Instant::now();
                    }
                }
                Err(Error::Api { status, errors }) if matches!(status, 401 | 403) => {
                    // Renewing a token the server has stopped honouring cannot
                    // work, however often it is tried.
                    tracing::error!(
                        status,
                        errors = %errors.join("; "),
                        "openbao refused to renew the token; it is no longer valid, so renewal stops"
                    );
                    return;
                }
                Err(e) => {
                    failures = failures.saturating_add(1);
                    tracing::warn!(error = %e, failures, "openbao token renewal failed; retrying");
                }
            }
        }
    }

    /// `auth/token/renew-self` for the current session.
    pub async fn renew_self(&self) -> Result<TokenInfo> {
        let answer = self
            .post("auth/token/renew-self", Value::Object(Default::default()))
            .await?;
        TokenInfo::from_auth(&answer)
    }

    /// `GET /v1/<path>`.
    pub async fn get(&self, path: &str) -> Result<Value> {
        let token = self.token()?;
        self.send(Method::GET, path, None, Some(&token), &[]).await
    }

    /// `POST /v1/<path>` with a JSON body.
    pub async fn post(&self, path: &str, body: Value) -> Result<Value> {
        let token = self.token()?;
        self.send(Method::POST, path, Some(&body), Some(&token), &[])
            .await
    }

    /// `POST /v1/<path>` asking OpenBao to **wrap** the answer: what comes
    /// back is a wrapping token, redeemable once within `wrap_ttl`, and the
    /// real answer waits in the cubbyhole behind it. The controller's half of
    /// the hand-off (0032 §2): `post_wrapped("auth/token/create", …, 5 s)`
    /// mints the credential a service redeems with [`Auth::Handoff`].
    pub async fn post_wrapped(
        &self,
        path: &str,
        body: Value,
        wrap_ttl: Duration,
    ) -> Result<Secret> {
        let token = self.token()?;
        let ttl = format!("{}s", wrap_ttl.as_secs().max(1));
        let mut answer = self
            .send(
                Method::POST,
                path,
                Some(&body),
                Some(&token),
                &[("x-vault-wrap-ttl", ttl)],
            )
            .await?;
        // Moved out of the answer: a copy would leave the original behind in a
        // tree that is not wiped when it is dropped.
        match answer.get_mut("wrap_info").and_then(|w| w.get_mut("token")) {
            Some(Value::String(token)) => Ok(Secret::from(std::mem::take(token))),
            _ => Err(Error::Shape(
                "no `wrap_info.token`: the answer was not wrapped".into(),
            )),
        }
    }

    /// `auth/token/revoke-accessor`: the controller's other half, at the
    /// service's exit. Revokes the token and every lease under it without
    /// ever having held the token.
    pub async fn revoke_accessor(&self, accessor: &str) -> Result<()> {
        self.post(
            "auth/token/revoke-accessor",
            serde_json::json!({ "accessor": accessor }),
        )
        .await
        .map(|_| ())
    }

    /// `POST /v1/<path>` with a JSON body and **no** token: logins.
    pub(crate) async fn post_anonymous(&self, path: &str, body: Value) -> Result<Value> {
        self.send(Method::POST, path, Some(&body), None, &[]).await
    }

    /// `sys/wrapping/unwrap` with `wrapping_token` as the token: the one
    /// request a wrapping token is good for. A token that was already
    /// redeemed — or never existed — is [`Error::HandoffStolen`].
    pub async fn unwrap(&self, wrapping_token: &str) -> Result<Value> {
        match self
            .send(
                Method::POST,
                "sys/wrapping/unwrap",
                None,
                Some(wrapping_token),
                &[],
            )
            .await
        {
            Ok(answer) => Ok(answer),
            Err(Error::Api { status, errors })
                if status == 400
                    && errors
                        .iter()
                        .any(|e| e.contains("wrapping token is not valid")) =>
            {
                Err(Error::HandoffStolen)
            }
            Err(e) => Err(e),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/v1/{}", self.base, path.trim_start_matches('/'))
    }

    /// One request and its whole answer, within [`Config::timeout`].
    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        token: Option<&str>,
        extra: &[(&str, String)],
    ) -> Result<Value> {
        let fields = request_fields(token, self.namespace.as_deref(), extra, body.is_some())?;
        let body = match body {
            Some(body) => serde_json::to_vec(body)
                .map_err(|e| Error::Shape(format!("the request is not JSON: {e}")))?
                .into(),
            None => Default::default(),
        };
        let url = self.url(path);
        let exchange = async {
            let mut response = self
                .http
                .request(method, &url, fields, body)
                .await
                .map_err(|e| Error::Http(e.to_string()))?;
            let status = response.head.status;
            if status.is_redirection() {
                let location = response
                    .head
                    .fields
                    .get("location")
                    .and_then(|value| std::str::from_utf8(value).ok())
                    .map_or_else(|| "an unreadable address".to_owned(), clip);
                return Err(Error::Api {
                    status: status.as_u16(),
                    errors: vec![format!(
                        "redirected to {location}: this client follows no redirects, so address \
                         the active node directly"
                    )],
                });
            }
            let body = read_capped(&mut response.body).await?;
            Ok((status, body))
        };
        let (status, body) = match self.exec.within(self.timeout, exchange).await {
            Some(answer) => answer?,
            None => {
                return Err(Error::Http(format!("no answer within {:?}", self.timeout)));
            }
        };
        if !status.is_success() {
            let errors = serde_json::from_slice::<Value>(&body)
                .ok()
                .and_then(|v| {
                    v.get("errors")?.as_array().map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .take(MAX_ERRORS)
                            .map(clip)
                            .collect()
                    })
                })
                .unwrap_or_else(|| {
                    let head = &body[..body.len().min(MAX_ERROR_TEXT * 4)];
                    vec![clip(&String::from_utf8_lossy(head))]
                });
            return Err(Error::Api {
                status: status.as_u16(),
                errors,
            });
        }
        if body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&body).map_err(|e| Error::Shape(format!("not JSON: {e}")))
    }
}

/// How long to wait before the next renewal attempt.
///
/// Half the token's TTL after a success, counted from when it was learned. After
/// a failure, a backoff that starts at five seconds and doubles, held to a
/// quarter of what is left of the token so that a few attempts still fit
/// before it expires, and to a minute once there is nothing left to protect.
fn renewal_wait(ttl: Duration, elapsed: Duration, failures: u32) -> Duration {
    const FLOOR: Duration = Duration::from_secs(1);
    const RETRY_FIRST: Duration = Duration::from_secs(5);
    const RETRY_LAST: Duration = Duration::from_secs(60);
    if failures == 0 {
        return (ttl / 2).saturating_sub(elapsed).max(FLOOR);
    }
    let backoff = RETRY_FIRST
        .saturating_mul(1u32 << (failures - 1).min(16))
        .min(RETRY_LAST);
    let remaining = ttl.saturating_sub(elapsed);
    if remaining.is_zero() {
        backoff
    } else {
        backoff.min((remaining / 4).max(FLOOR))
    }
}

/// The fields of one request. The token travels as a sensitive field, which
/// HTTP/2 and HTTP/3 never put into a compression table and nothing prints.
fn request_fields(
    token: Option<&str>,
    namespace: Option<&str>,
    extra: &[(&str, String)],
    json: bool,
) -> Result<Fields> {
    let mut fields = Fields::new();
    let field = |e: weida_http::Error| Error::Shape(format!("a request field: {e}"));
    fields.append("x-vault-request", "true").map_err(field)?;
    if let Some(token) = token {
        fields
            .append_sensitive("x-vault-token", token.to_owned())
            .map_err(|_| Error::Shape("the token is not a valid header value".into()))?;
    }
    if let Some(namespace) = namespace {
        fields
            .append("x-vault-namespace", namespace.to_owned())
            .map_err(field)?;
    }
    for (name, value) in extra {
        fields.append(name, value.clone()).map_err(field)?;
    }
    if json {
        fields
            .append("content-type", "application/json")
            .map_err(field)?;
    }
    Ok(fields)
}

/// Reads an answer's body, up to [`MAX_RESPONSE_BYTES`] and no further.
async fn read_capped(body: &mut BodyReader) -> Result<Vec<u8>> {
    let too_large = || {
        Error::Http(format!(
            "the answer is larger than the {MAX_RESPONSE_BYTES}-byte cap"
        ))
    };
    if body
        .content_length()
        .is_some_and(|len| len > MAX_RESPONSE_BYTES as u64)
    {
        return Err(too_large());
    }
    match body.collect(MAX_RESPONSE_BYTES).await {
        Ok(bytes) => Ok(bytes.to_vec()),
        Err(weida_http::Error::TooLarge { .. }) => Err(too_large()),
        Err(e) => Err(Error::Http(e.to_string())),
    }
}

/// `text` cut to [`MAX_ERROR_TEXT`] bytes at a character boundary, with
/// control characters made spaces so a server cannot write a line of its own
/// into a log.
fn clip(text: &str) -> String {
    let mut out = String::new();
    for c in text.chars() {
        if out.len() + c.len_utf8() > MAX_ERROR_TEXT {
            out.push('…');
            break;
        }
        out.push(if c.is_control() { ' ' } else { c });
    }
    out
}

impl std::fmt::Debug for OpenBao {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenBao")
            .field("base", &self.base)
            .field(
                "authenticated",
                &self.session.read().expect("session poisoned").is_some(),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    #[test]
    fn a_renewal_waits_half_the_ttl_from_when_it_was_learned() {
        assert_eq!(
            renewal_wait(SECOND * 3600, Duration::ZERO, 0),
            SECOND * 1800
        );
        assert_eq!(
            renewal_wait(SECOND * 3600, SECOND * 600, 0),
            SECOND * 1200,
            "the time already elapsed counts"
        );
        assert_eq!(
            renewal_wait(SECOND * 2, Duration::ZERO, 0),
            SECOND,
            "never faster than once a second"
        );
    }

    #[test]
    fn a_failed_renewal_is_retried_inside_what_is_left_of_the_token() {
        // Four seconds, failed at the half: the retry must land before the
        // token is gone, not another half of its life later.
        let wait = renewal_wait(SECOND * 4, SECOND * 2, 1);
        assert!(wait <= SECOND, "{wait:?}");
        // A long token backs off from five seconds and doubles.
        let ttl = SECOND * 3600;
        let elapsed = SECOND * 1800;
        assert_eq!(renewal_wait(ttl, elapsed, 1), SECOND * 5);
        assert_eq!(renewal_wait(ttl, elapsed, 2), SECOND * 10);
        assert_eq!(renewal_wait(ttl, elapsed, 3), SECOND * 20);
        assert_eq!(renewal_wait(ttl, elapsed, 30), SECOND * 60, "capped");
        // Never longer than a quarter of what is left.
        assert_eq!(
            renewal_wait(SECOND * 100, SECOND * 90, 5),
            SECOND * 2 + SECOND / 2
        );
    }

    #[test]
    fn an_expired_token_is_retried_at_the_slow_pace() {
        assert_eq!(renewal_wait(SECOND * 10, SECOND * 20, 1), SECOND * 5);
        assert_eq!(renewal_wait(SECOND * 10, SECOND * 20, 20), SECOND * 60);
    }

    #[test]
    fn what_a_server_writes_into_an_error_is_cut_and_flattened() {
        let long = "x".repeat(MAX_ERROR_TEXT * 3);
        let clipped = clip(&long);
        assert!(clipped.len() <= MAX_ERROR_TEXT + '…'.len_utf8());
        assert!(clipped.ends_with('…'));

        assert_eq!(clip("one\nforged line\r\n"), "one forged line  ");

        // Two-byte characters: the cap falls on a boundary, never inside one,
        // and what is kept is whole characters.
        let wide = "\u{e9}".repeat(MAX_ERROR_TEXT);
        let clipped = clip(&wide);
        assert!(clipped.len() <= MAX_ERROR_TEXT + '…'.len_utf8());
        assert!(clipped.trim_end_matches('…').chars().all(|c| c == '\u{e9}'));
    }

    #[test]
    fn the_token_never_prints() {
        let fields = request_fields(Some("s.token"), None, &[], true).expect("fields");
        assert_eq!(
            fields.get("x-vault-token").map(|v| &v[..]),
            Some(&b"s.token"[..])
        );
        assert!(!format!("{fields:?}").contains("s.token"));
        assert!(matches!(
            request_fields(Some("s.tok\nen"), None, &[], false),
            Err(Error::Shape(_))
        ));
    }
}
