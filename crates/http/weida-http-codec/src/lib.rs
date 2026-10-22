//! HTTP/1.1, HPACK, HTTP/2 framing, QPACK and HTTP/3 framing, sans-I/O.
//!
//! This crate is the codec under `weida-http`, the HTTP client and server
//! library ([0044](../../../docs/decisions/0044-weida-http.md)). It is
//! **sans-I/O and weida-free**: no sockets, no executor, no dependency, not
//! even on `weida-core`. What it exports are byte-for-byte encoders and
//! decoders for the published RFCs, so that it can be checked against their
//! examples rather than against weida's opinion of them.
//!
//! # Shape
//!
//! * [`h1`]: HTTP/1.1 request and response heads, body framing, chunked
//!   bodies (RFC 9112).
//! * [`h2`]: HTTP/2 frames (RFC 9113 §4, §6).
//! * [`hpack`]: HTTP/2 field compression with its [`huffman`] code
//!   (RFC 7541).
//! * [`fields`]: the field name and value grammar every version shares
//!   (RFC 9110 §5).
//! * [`uri`]: `http`/`https` URLs for a client, origin-form targets for a
//!   server.
//! * [`date`]: the IMF-fixdate a server writes into `date`.
//! * [`Limits`]: the caller's bounds, an argument to every decode.
//! * [`error`]: why bytes were rejected, split by direction.
//!
//! # Reading a stream
//!
//! ```
//! use weida_http_codec::{DecodeError, Limits, h1};
//!
//! let wire = b"GET /hello.txt HTTP/1.1\r\nHost: www.example.com\r\n\r\n";
//!
//! // A head split across reads is incomplete until its empty line arrives.
//! assert!(matches!(
//!     h1::decode_request(&wire[..20], Limits::DEFAULT),
//!     Err(DecodeError::Incomplete { .. })
//! ));
//!
//! let (head, used) = h1::decode_request(wire, Limits::DEFAULT).expect("a request");
//! assert_eq!(used, wire.len());
//! assert_eq!(head.method, b"GET");
//! assert_eq!(head.target, b"/hello.txt");
//! assert_eq!(head.fields[0].value, b"www.example.com");
//! assert_eq!(h1::request_framing(&head.fields), Ok(h1::Framing::Empty));
//! ```
//!
//! # Bounds
//!
//! HTTP declares sizes in ASCII and in prefixed integers: a few octets can
//! announce a body, a frame or a string of any size, and a head is as long as
//! the peer keeps writing. Every decode entry point therefore takes [`Limits`]
//! as an **argument**, and a declared size is refused from the declaration
//! alone, before anything is reserved; a delimited element (a head, a chunk
//! line) is refused once the cap's worth of octets has been searched.
//!
//! # Where this codec departs from the text
//!
//! * **Obsolete line folding is refused in both directions.** RFC 9112 §5.2
//!   lets a recipient of a *response* replace obs-fold with spaces; this
//!   codec refuses it as it does in a request (`"obsolete line folding"`),
//!   so that one parser serves both directions. `tests/golden_vectors.rs`
//!   pins it for both.
//! * **A transfer coding other than `chunked` is refused.** RFC 9112 §6.1
//!   allows codings such as `gzip, chunked`; a library that applies no
//!   content coding cannot deliver such a body, so the framing functions say
//!   `"transfer coding"` instead of guessing.
//! * **A bare LF ends a line on input and is never written** (RFC 9112 §2.2
//!   permits the former).
//!
//! # Sources
//!
//! Every rule comes from `docs/research/http.md`, which cites RFC 9110,
//! RFC 9112, RFC 9113 (June 2022) and RFC 7541 (May 2015).

pub mod date;
pub mod error;
pub mod fields;
pub mod h1;
pub mod h2;
pub mod hpack;
pub mod huffman;
pub mod limits;
mod prefix;
pub mod uri;

pub use error::{DecodeError, EncodeError};
pub use limits::Limits;
