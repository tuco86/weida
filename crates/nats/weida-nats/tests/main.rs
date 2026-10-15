//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod support;

mod driver;
mod handshake;
mod interop_nats_server;
mod refusals;
mod request_reply;
mod subscriptions;
