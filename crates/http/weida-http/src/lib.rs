//! An HTTP client and server on a sans-I/O codec, usable with no weida in the
//! picture.
//!
//! This crate is the library of weida's HTTP family
//! ([0044](../../../docs/decisions/0044-weida-http.md)): HTTP/1.1 over TCP and
//! TLS, on `weida-http-codec` for every octet and `weida-runtime` for every
//! task, timer and name lookup. It depends on neither `weida` nor
//! `weida-protocol`.
//!
//! # Shape
//!
//! * [`Client`]: requests to any origin, with idle HTTP/1.1 connections kept
//!   per origin. [`Client::request`] sends a whole body,
//!   [`Client::open`] streams one.
//! * [`Response`] and [`BodyReader`]: a response head and its body, read in
//!   chunks as they arrive or collected under the caller's cap.
//! * [`Fields`], [`Method`], [`Status`], [`Url`]: the vocabulary, validated
//!   on entry so nothing the wire cannot carry gets as far as an encoder.
//! * [`ClientOptions`]: the caller's trust anchors, timeouts and bounds.
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
//! speaks to no proxy, and sends no `CONNECT` or `Upgrade`
//! (`docs/decisions/0044-weida-http.md` §6).

#![warn(missing_docs)]

mod body;
mod client;
mod dial;
mod error;
mod h1;
mod h2;
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
