//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod common;

mod scripted;

/// Tests that touch the filesystem, network, processes or real time: the
/// integration tier (`.config/nextest.toml`).
mod io {
    mod bao_dev;
    mod scripted;
}
