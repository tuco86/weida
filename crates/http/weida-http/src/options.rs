//! Client and server options.
//!
//! # Which bound is whose
//!
//! | bound | holds back | default |
//! | --- | --- | --- |
//! | `Limits::max_head_bytes` | a peer's head or field section, a trailer section | 64 KiB |
//! | `Limits::max_fields` | a peer's field count | 128 |
//! | `Limits::max_chunk_line` | a peer's chunk-size line | 1 KiB |
//! | `Limits::max_frame_size` | a peer's HTTP/2 frame | 16 384 |
//! | `stream_window` | what one HTTP/2 stream may send us before we read it | 1 MiB |
//! | `connection_window` | what one HTTP/2 connection may send us in total | 4 MiB |
//! | `ClientOptions::max_idle_per_origin` | idle HTTP/1.1 connections kept per origin | 4 |
//! | `ClientOptions::max_resolved_addresses` | addresses tried for one name | 8 |
//! | `ServerOptions::max_connections` | connections served at once | 1024 |
//! | `ServerOptions::accept_queue` | exchanges waiting for `accept` | 64 |
//! | `ServerOptions::max_concurrent_streams` | HTTP/2 streams a client may open at once | 100 |
//!
//! A body is never bounded here: it is streamed, and a caller that collects
//! one names its own cap (`BodyReader::collect`).

use std::time::Duration;

use weida_http_codec::Limits;

use crate::error::{Error, Result};

/// Which HTTP version a client speaks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Protocol {
    /// HTTP/2 when the server agrees to it by ALPN on `https`, HTTP/1.1
    /// otherwise.
    #[default]
    Auto,
    /// HTTP/1.1 only.
    Http1,
    /// HTTP/2 only, over TLS; a server that does not agree is an error.
    Http2,
    /// HTTP/3 over QUIC; needs the `h3` feature.
    Http3,
}

/// The largest flow-control window HTTP/2 allows: 2^31 - 1.
const MAX_WINDOW: u32 = (1 << 31) - 1;

fn check_window(name: &str, value: u32) -> Result<()> {
    if value == 0 || value > MAX_WINDOW {
        return Err(Error::Configuration(format!(
            "{name} must be between 1 and 2^31 - 1, not {value}"
        )));
    }
    Ok(())
}

fn check_count(name: &str, value: usize) -> Result<()> {
    if value == 0 {
        return Err(Error::Configuration(format!("{name} must not be 0")));
    }
    Ok(())
}

fn check_limits(limits: &Limits) -> Result<()> {
    check_count("limits.max_head_bytes", limits.max_head_bytes)?;
    check_count("limits.max_fields", limits.max_fields)?;
    check_count("limits.max_chunk_line", limits.max_chunk_line)?;
    if !(16_384..=16_777_215).contains(&limits.max_frame_size) {
        return Err(Error::Configuration(format!(
            "limits.max_frame_size must be between 16384 and 16777215, not {}",
            limits.max_frame_size
        )));
    }
    Ok(())
}

/// A client's options.
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// The TLS configuration for `https`: the caller's trust anchors. The
    /// client clones it and sets only `alpn_protocols`. `None` refuses
    /// `https` URLs.
    #[cfg(feature = "tls")]
    pub tls: Option<std::sync::Arc<rustls::ClientConfig>>,
    /// Which version to speak.
    pub protocol: Protocol,
    /// The name lookup, the TCP connect, and the TLS or QUIC handshake,
    /// together.
    pub connect_timeout: Duration,
    /// How long an idle HTTP/1.1 connection is kept for reuse.
    pub idle_timeout: Duration,
    /// How many idle HTTP/1.1 connections are kept per origin.
    pub max_idle_per_origin: usize,
    /// How many of a name's addresses are tried.
    pub max_resolved_addresses: usize,
    /// What one HTTP/2 stream may send before it is read.
    pub stream_window: u32,
    /// What one HTTP/2 connection may send before it is read.
    pub connection_window: u32,
    /// Bounds on what a server sends.
    pub limits: Limits,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "tls")]
            tls: None,
            protocol: Protocol::Auto,
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(90),
            max_idle_per_origin: 4,
            max_resolved_addresses: 8,
            stream_window: 1 << 20,
            connection_window: 4 << 20,
            limits: Limits::DEFAULT,
        }
    }
}

impl ClientOptions {
    /// Refuses zero counts and windows outside `1..=2^31 - 1`.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] naming the option.
    pub fn validate(&self) -> Result<()> {
        check_count("max_resolved_addresses", self.max_resolved_addresses)?;
        check_window("stream_window", self.stream_window)?;
        check_window("connection_window", self.connection_window)?;
        check_limits(&self.limits)
    }
}

/// A server's options.
#[derive(Clone, Debug)]
pub struct ServerOptions {
    /// Connections served at once; a connection beyond it waits in the
    /// listen backlog.
    pub max_connections: usize,
    /// Exchanges that wait for `Server::accept`; a full queue makes
    /// connections wait.
    pub accept_queue: usize,
    /// HTTP/2 streams a client may open at once.
    pub max_concurrent_streams: u32,
    /// How long an HTTP/1.1 head, an HTTP/2 preface, or a TLS or QUIC
    /// handshake may take.
    pub header_timeout: Duration,
    /// How long a connection may sit idle between requests.
    pub idle_timeout: Duration,
    /// How long `Server::close` waits for exchanges in flight.
    pub close_timeout: Duration,
    /// What one HTTP/2 stream may send before it is read.
    pub stream_window: u32,
    /// What one HTTP/2 connection may send before it is read.
    pub connection_window: u32,
    /// Bounds on what a client sends.
    pub limits: Limits,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            accept_queue: 64,
            max_concurrent_streams: 100,
            header_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            close_timeout: Duration::from_secs(5),
            stream_window: 1 << 20,
            connection_window: 4 << 20,
            limits: Limits::DEFAULT,
        }
    }
}

impl ServerOptions {
    /// Refuses zero counts and windows outside `1..=2^31 - 1`.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] naming the option.
    pub fn validate(&self) -> Result<()> {
        check_count("max_connections", self.max_connections)?;
        check_count("accept_queue", self.accept_queue)?;
        check_count(
            "max_concurrent_streams",
            self.max_concurrent_streams as usize,
        )?;
        check_window("stream_window", self.stream_window)?;
        check_window("connection_window", self.connection_window)?;
        check_limits(&self.limits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_counts_and_oversized_windows_are_refused() {
        assert!(ClientOptions::default().validate().is_ok());
        assert!(ServerOptions::default().validate().is_ok());
        let o = ClientOptions {
            stream_window: 1 << 31,
            ..ClientOptions::default()
        };
        assert!(
            matches!(o.validate(), Err(Error::Configuration(m)) if m.contains("stream_window"))
        );
        let o = ServerOptions {
            accept_queue: 0,
            ..ServerOptions::default()
        };
        assert!(matches!(o.validate(), Err(Error::Configuration(m)) if m.contains("accept_queue")));
    }
}
