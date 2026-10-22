//! An HTTP/1.1, HTTP/2 and HTTP/3 client and server on a sans-I/O codec,
//! usable with no weida in the picture.
//!
//! This crate is the library of weida's HTTP family
//! ([0044](../../../docs/decisions/0044-weida-http.md)): HTTP/1.1 over TCP and
//! TLS, HTTP/2 over TLS by ALPN `h2`, and HTTP/3 over QUIC behind the
//! feature `h3`, on `weida-http-codec` for every octet and `weida-runtime`
//! for every task, timer and name lookup. It depends on neither `weida` nor
//! `weida-protocol`.
//!
//! # Shape
//!
//! * [`Client`]: requests to any origin. Idle HTTP/1.1 connections are kept
//!   per origin, one HTTP/2 or HTTP/3 connection is shared per origin.
//!   [`Client::request`] sends a whole body, [`Client::open`] streams one.
//! * [`Server`]: listeners and connections; [`Server::accept`] hands out one
//!   [`Exchange`] per request, answered through its [`Responder`].
//! * [`BodyReader`] and [`BodyWriter`]: bodies streamed in chunks, with flow
//!   control behind them, or collected under the caller's cap.
//! * [`Fields`], [`Method`], [`Status`], [`Url`]: the vocabulary, validated
//!   on entry so nothing the wire cannot carry gets as far as an encoder.
//! * [`ClientOptions`] and [`ServerOptions`]: the caller's protocol choice,
//!   timeouts and bounds; the trust anchors are the caller's rustls
//!   configurations.
//!
//! # A request
//!
//! ```no_run
//! # async fn demo() -> weida_http::Result<()> {
//! use weida_http::{Client, ClientOptions, Fields, Method};
//!
//! let exec = weida_runtime::Exec::current()?;
//! let client = Client::new(&exec, ClientOptions::default())?;
//! let mut response = client
//!     .request(Method::GET, "http://127.0.0.1:8080/", Fields::new(), Default::default())
//!     .await?;
//! let body = response.body.collect(1 << 20).await?;
//! println!("{} {}", response.head.status, String::from_utf8_lossy(&body));
//! # Ok(())
//! # }
//! ```
//!
//! # What it does not do
//!
//! It follows no redirects, stores no cookies, applies no content coding,
//! speaks to no proxy, speaks HTTP/2 only over TLS (no h2c), never pushes,
//! finds HTTP/3 only when asked (no Alt-Svc), uses no QPACK dynamic table,
//! and sends no `CONNECT` or `Upgrade` (`docs/decisions/0044-weida-http.md`
//! §6).

#![warn(missing_docs)]

mod body;
mod client;
mod dial;
mod error;
mod h1;
// HTTP/2 is spoken only over TLS (no h2c, 0044 §4).
#[cfg(feature = "tls")]
mod h2;
#[cfg(feature = "h3")]
mod h3;
mod options;
mod pool;
mod server;
mod transport;
mod types;

pub use body::{BodyReader, BodyWriter};
pub use client::{Client, OpenRequest, Response};
pub use error::{Error, Result};
pub use options::{ClientOptions, Protocol, ServerOptions};
pub use server::{Exchange, Responder, Server};
pub use types::{Fields, Method, RequestHead, ResponseHead, Status, Url, Version};
pub use weida_http_codec::Limits;
pub use weida_http_codec::uri::Scheme;
