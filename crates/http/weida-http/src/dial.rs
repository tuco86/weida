//! Opening a connection: name lookup, TCP connect, TLS handshake, under one
//! budget.
//!
//! The shape is `weida-nats`'s: every address the resolver offers is tried in
//! its order, each attempt but the last gets half of what is left, and the
//! lookup and every attempt share one budget, so a handful of addresses that
//! swallow packets cannot hold a caller for the timeout each.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::transport::Stream;

/// What is left of one time budget, spent a step at a time.
pub(crate) struct Budget {
    ends: Option<Instant>,
    whole: Duration,
}

impl Budget {
    pub(crate) fn start(exec: &Exec, whole: Duration) -> Self {
        Self {
            ends: exec.now().checked_add(whole),
            whole,
        }
    }

    pub(crate) fn left(&self, exec: &Exec) -> Duration {
        self.ends.map_or(self.whole, |ends| {
            ends.saturating_duration_since(exec.now())
        })
    }
}

/// Resolves `host` and connects to one of its addresses within `budget`.
pub(crate) async fn connect(
    exec: &Exec,
    host: &str,
    port: u16,
    max_addresses: usize,
    budget: &Budget,
) -> Result<Stream> {
    let addrs = resolve(exec, host, port, max_addresses, budget).await?;
    dial(exec, &addrs, budget).await
}

/// The addresses of `host`, inside what is left of `budget`.
pub(crate) async fn resolve(
    exec: &Exec,
    host: &str,
    port: u16,
    max_addresses: usize,
    budget: &Budget,
) -> Result<Vec<SocketAddr>> {
    let lookup = exec.resolve(host, port, max_addresses);
    match exec.within(budget.left(exec), lookup).await {
        Some(answer) => Ok(answer?),
        None => Err(Error::Timeout {
            step: "the name lookup",
        }),
    }
}

/// Tries `addrs` in order inside what is left of `budget`.
async fn dial(exec: &Exec, addrs: &[SocketAddr], budget: &Budget) -> Result<Stream> {
    if addrs.is_empty() {
        return Err(Error::Runtime(weida_core::Error::InvalidAddress(
            "the resolver offered no addresses".into(),
        )));
    }
    let mut last = None;
    for (index, addr) in addrs.iter().enumerate() {
        let left = budget.left(exec);
        let share = if index + 1 < addrs.len() {
            left / 2
        } else {
            left
        };
        if share.is_zero() {
            break;
        }
        // `TcpStream::connect` registers the socket with the reactor as it is
        // built; spawning puts the constructor where the reactor is.
        let mut attempt = exec.spawn(TcpStream::connect(*addr));
        match exec.within(share, &mut attempt).await {
            Some(Ok(Ok(stream))) => {
                // Nagle off: a request head and its answer are short writes
                // that each wait for the other side.
                let _ = stream.set_nodelay(true);
                return Ok(Stream::Plain(stream));
            }
            Some(Ok(Err(error))) => last = Some(Error::Io(error)),
            Some(Err(error)) => {
                last = Some(Error::Runtime(weida_core::Error::Runtime(format!(
                    "connect task failed: {error}"
                ))));
            }
            None => {
                attempt.abort();
                last = Some(Error::Timeout {
                    step: "the TCP connect",
                });
            }
        }
    }
    Err(last.unwrap_or(Error::Timeout {
        step: "the TCP connect",
    }))
}

/// Puts TLS under a connected stream, offering `alpn`, within what is left of
/// `budget`. The caller's `config` is cloned and only its ALPN list is set;
/// `host` is validated against the certificate and sent as SNI (an IP literal
/// becomes an IP server name and is not sent).
#[cfg(feature = "tls")]
pub(crate) async fn upgrade_tls(
    exec: &Exec,
    stream: Stream,
    config: &std::sync::Arc<rustls::ClientConfig>,
    host: &str,
    alpn: &[&[u8]],
    budget: &Budget,
) -> Result<Stream> {
    let Stream::Plain(tcp) = stream else {
        return Err(Error::Tls("the stream is already under TLS".into()));
    };
    let mut config = (**config).clone();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    let name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|_| Error::Tls(format!("{host} is not a valid server name")))?;
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    match exec
        .within(budget.left(exec), connector.connect(name, tcp))
        .await
    {
        Some(Ok(tls)) => Ok(Stream::Tls(Box::new(tls))),
        Some(Err(error)) => Err(Error::Tls(error.to_string())),
        None => Err(Error::Timeout {
            step: "the TLS handshake",
        }),
    }
}
