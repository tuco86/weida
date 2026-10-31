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

mod error;
mod handle;
mod op;
mod poll;
mod ready;
mod runtime;
mod try_handle;

use std::ffi::c_char;

pub use error::*;
pub use poll::{WEIDA_POLLIN, weida_poll, weida_poll_item};
pub use runtime::{
    weida_runtime, weida_runtime_config, weida_runtime_config_drop, weida_runtime_config_new,
    weida_runtime_config_set_shutdown_timeout_ms, weida_runtime_config_set_worker_threads,
    weida_runtime_drop, weida_runtime_owned, weida_runtime_shutdown, weida_runtime_shutdown_op,
    weida_runtime_shutdown_op_drop, weida_runtime_shutdown_op_fd, weida_runtime_shutdown_op_wait,
};

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
