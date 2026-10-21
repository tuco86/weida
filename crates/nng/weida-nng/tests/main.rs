//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod local_transports;
mod options;
mod raw_device;
mod tls;

/// Tests that touch the filesystem, network, processes or real time: the
/// integration tier (`.config/nextest.toml`).
mod io {
    mod bus;
    mod directions;
    mod interop_nng;
    mod local_transports;
    mod options;
    mod pair;
    mod pipeline;
    mod pubsub;
    mod raw_device;
    mod reqrep;
    mod survey;
    mod tls;
}
