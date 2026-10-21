//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.
//!
//! Each file includes its own copy of `common`, because each adds methods of its
//! own to the types there.

#![allow(
    clippy::duplicate_mod,
    reason = "each test file includes `common` on purpose, see above"
)]

mod admission;
mod cursors;
mod delivery;
mod refusals;
mod settlement;
mod stalls;

/// Tests that touch the filesystem, network, processes or real time: the
/// integration tier (`.config/nextest.toml`).
mod io {
    mod backlog;
    mod stalls;
}
