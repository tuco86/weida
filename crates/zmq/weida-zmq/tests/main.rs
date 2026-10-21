//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod common;

mod inproc;
mod ipc;
mod plain_zap;
mod wakeups;
mod zguide_clone;

/// Tests that touch the filesystem, network, processes or real time: the
/// integration tier (`.config/nextest.toml`).
mod io {
    mod interop_libzmq;
    mod interop_zmqrs;
    mod ipc;
    mod plain_zap;
    mod router_identity;
    mod split;
    mod thread_rule;
    mod zguide_clone;
    mod zguide_majordomo;
    mod zguide_pirates;
}
