//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod common;

mod alpn;
mod blocking;
mod bus;
mod cursors;
mod drain;
mod flows;
mod foreign_executor;
mod guide;
mod identity;
mod interrupted;
mod pair;
mod pubsub;
mod pushpull;
mod radio;
mod raw_streams;
mod reconnect;
mod reorder;
mod reqrep;
mod sim;
mod stats;
mod streams;
mod survey;
#[cfg(feature = "swarm")]
mod swarm;
mod transports;

/// Tests that touch the filesystem, network, processes or real time: the
/// integration tier (`.config/nextest.toml`).
mod io {
    mod accept_limits;
    mod blocking;
    mod cli;
    mod cursors;
    mod dedup;
    mod drain;
    mod guide;
    mod hostile;
    mod identity;
    mod identity_source;
    mod interrupted;
    mod local_hostile;
    mod pubsub;
    mod radio;
    mod raw_streams;
    mod reconnect;
    mod redial_storm;
    mod reorder;
    mod stats;
    mod streams;
    mod tls_trust;
    mod transports;
    mod unsafe_guard;
}
