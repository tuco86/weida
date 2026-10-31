//! The C ABI driven through its exports, as a C caller would.

/// Tests that touch sockets, threads and signals: the integration tier
/// (`.config/nextest.toml`).
mod io {
    mod abi;
}
