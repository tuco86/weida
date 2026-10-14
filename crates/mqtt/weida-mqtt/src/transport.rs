//! The byte stream a connection runs on: plain TCP on 1883, TLS on 8883.
//!
//! # What TLS is and is not, here
//!
//! "The MQTT protocol is not trust symmetrical. When using basic
//! authentication, there is no mechanism for the Client to authenticate the
//! Server" (5.4.3) [mqtt5 §10]. TLS is the usual answer to that half, with SNI
//! recommended for a server serving several hostnames - and it is the *only*
//! answer this protocol offers, because MQTT has no server-side credential of
//! its own.
//!
//! What TLS proves is what the transport peer is. It says nothing about who
//! composed a message: an application message that arrives on a TLS
//! connection carries no claim about its publisher, because MQTT has no
//! per-message identity at all ([mqtt5 §12/P14]). So there is no conversion
//! from the TLS peer to any identity type in this crate, and no accessor on a
//! [`crate::Delivery`] that returns one.
//!
//! # Where the trust anchors are not
//!
//! They are the caller's. `TlsOptions` takes a `rustls::ClientConfig` the
//! application built, because which certificates an application trusts is the
//! application's decision and a messaging library that picked for it is one
//! that cannot be audited. This crate adds the tokio adapter over the rustls
//! already in this workspace's graph and **no second TLS stack and no second
//! cryptographic backend**.
//!
//! `TlsOptions` is named in backticks rather than linked because it exists
//! only with the `tls` feature on, and an intra-doc link to a `cfg`-ed item
//! breaks `cargo doc --no-default-features` — which is a configuration this
//! crate supports and therefore documents.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// What a caller must supply for TLS.
///
/// Cloneable and cheap to clone: the config is behind an `Arc`, as rustls
/// intends, so one config serves every connection an application opens.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct TlsOptions {
    /// The caller's rustls configuration: trust anchors, client certificate,
    /// ALPN, everything that decides what "a valid server" means.
    pub config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    /// The name the certificate is validated against and the name that goes in
    /// SNI, which 5.4.3 recommends for a server serving several hostnames
    /// [mqtt5 §10].
    ///
    /// `None` uses the host from the connect address, which is right whenever
    /// the address is a DNS name. It has to be settable because it is
    /// **wrong** whenever the address is an IP literal or a tunnel endpoint
    /// and the certificate names something else - and a client that could not
    /// say so would have to choose between not validating and not connecting.
    pub server_name: Option<String>,
}

#[cfg(feature = "tls")]
impl TlsOptions {
    /// TLS with `config`, validating against the connect address's host.
    #[must_use]
    pub fn new(config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>) -> TlsOptions {
        TlsOptions {
            config,
            server_name: None,
        }
    }

    /// The same, validating against `name` instead of the connect address.
    #[must_use]
    pub fn with_server_name(mut self, name: impl Into<String>) -> TlsOptions {
        self.server_name = Some(name.into());
        self
    }
}

#[cfg(feature = "tls")]
impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The config is not printed: it holds the client's private key.
        f.debug_struct("TlsOptions")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

/// The stream one connection reads and writes.
///
/// An enum rather than a boxed trait object: there are exactly two cases, the
/// dispatch is one branch per read, and a `Box<dyn AsyncRead + AsyncWrite>`
/// would add an allocation and a vtable call to every packet for no gain.
#[derive(Debug)]
pub enum Stream {
    /// Plain TCP, which is MQTT on 1883.
    Plain(TcpStream),
    /// TCP under TLS, which is MQTT on 8883.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Stream {
    /// The peer's address, for a log line.
    ///
    /// # Errors
    ///
    /// Whatever the socket reported.
    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            Stream::Plain(stream) => stream.peer_addr(),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => stream.get_ref().0.peer_addr(),
        }
    }

    /// Whether the stream is encrypted.
    ///
    /// Not cosmetic: 5.0 permits a Password with no User Name (3.1.2.9)
    /// [mqtt5 §10] and both cross the wire in the clear, so a caller deciding
    /// whether to send credentials at all wants to be able to ask.
    #[must_use]
    pub const fn is_encrypted(&self) -> bool {
        match self {
            Stream::Plain(_) => false,
            #[cfg(feature = "tls")]
            Stream::Tls(_) => true,
        }
    }

    /// Nagle off. An MQTT handshake is a sequence of small packets that each
    /// need an answer, and coalescing them adds a round trip's delay to every
    /// step.
    pub(crate) fn set_nodelay(&self) -> std::io::Result<()> {
        match self {
            Stream::Plain(stream) => stream.set_nodelay(true),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => stream.get_ref().0.set_nodelay(true),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Puts TLS under an already-connected TCP stream.
///
/// The handshake completes **before the CONNECT is written**, which is the
/// whole ordering requirement: "the Client MUST NOT send any MQTT packets
/// before the TLS handshake completes" is what makes the credentials in
/// CONNECT worth sending at all.
///
/// # Errors
///
/// [`crate::Error::Configuration`] for a `server_name` that is not a valid
/// DNS name or IP address, and [`crate::Error::Io`] for a handshake the peer
/// or the validation refused.
#[cfg(feature = "tls")]
pub(crate) async fn upgrade(
    stream: Stream,
    options: &TlsOptions,
    host: &str,
) -> crate::error::Result<Stream> {
    let Stream::Plain(tcp) = stream else {
        return Err(crate::Error::Configuration(
            "the stream is already under TLS".into(),
        ));
    };
    let name = options.server_name.as_deref().unwrap_or(host);
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from(name.to_owned())
        .map_err(|_| {
            crate::Error::Configuration(format!(
                "{name} is not a valid server name to validate a certificate against; \
                 set TlsOptions::server_name where the connect address is an IP literal"
            ))
        })?;
    let connector = tokio_rustls::TlsConnector::from(options.config.clone());
    let tls = connector.connect(server_name, tcp).await?;
    Ok(Stream::Tls(Box::new(tls)))
}
