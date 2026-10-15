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
mod backlog;
mod cursors;
mod delivery;
mod refusals;
mod settlement;
mod stalls;
