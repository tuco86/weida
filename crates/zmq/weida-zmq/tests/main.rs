//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod common;

mod inproc;
mod interop_libzmq;
mod interop_zmqrs;
mod ipc;
mod plain_zap;
mod router_identity;
mod split;
mod thread_rule;
mod wakeups;
mod zguide_clone;
mod zguide_majordomo;
mod zguide_pirates;
