//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.
//!
//! Each file includes its own copy of `support`, because each adds methods of its
//! own to the types there.

#![allow(
    clippy::duplicate_mod,
    reason = "each test file includes `support` on purpose, see above"
)]

mod cancel_and_retention;
mod credit_enforcement;
mod disposition;
mod handshake;
mod hostile_attach;
mod interop_fe2o3;
mod interop_rabbitmq;
mod link;
mod protocol_state;
mod session;
mod stalled_peer;
mod transfer;
