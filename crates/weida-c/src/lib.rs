//! The C ABI of weida: `libweida` and `weida.h`.
//!
//! This crate is the boundary every non-Rust language crosses
//! ([decisions/0045](../../../docs/decisions/0045-the-c-abi-is-the-boundary.md)).
//! Its rules (R1 to R13: names, returns, the async mapping, timeouts,
//! handles and threads, values, errors, panics, readiness, signals) are
//! normative and live in `docs/c/README.md`; the header
//! `include/weida.h` is generated from this crate by `header.sh`.
//!
//! Every opaque C type is a Rust struct of exactly its C name, which is why
//! the crate allows non-camel-case type names.

#![allow(non_camel_case_types)]

mod bus;
mod config;
mod cursor;
mod error;
mod flow;
mod handle;
mod identity;
mod listener;
mod op;
mod pair;
mod peer;
mod poll;
mod pubsub;
mod push;
mod radio;
mod ready;
mod relay;
mod requester;
mod resolver;
mod runtime;
mod segment;
mod stats;
mod survey;
mod tls;
mod transfer;
mod try_handle;

use std::ffi::c_char;

pub use bus::*;
pub use config::*;
pub use cursor::*;
pub use error::*;
pub use flow::*;
pub use identity::*;
pub use listener::*;
pub use pair::*;
pub use peer::*;
pub use poll::*;
pub use pubsub::*;
pub use push::*;
pub use radio::*;
pub use relay::*;
pub use requester::*;
pub use resolver::*;
pub use runtime::*;
pub use segment::*;
pub use stats::*;
pub use survey::*;
pub use tls::*;
pub use transfer::*;
/// The library's version, e.g. `"0.1.0-alpha.11-dev"`. The string is
/// static.
///
/// # Safety
///
/// Always safe to call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}
