//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod common;

mod alpn;
mod bus;
mod cursors;
mod drain;
mod flows;
mod foreign_executor;
mod guide;
mod hostile;
mod identity;
mod interrupted;
mod local_hostile;
mod pair;
mod pubsub;
mod pushpull;
mod radio;
mod raw_streams;

/// Raw `quinn` peers on the simulated network, speaking hand-encoded frames
/// to a runtime under test on a paused clock.
mod raw_peers {
    mod cursors;
    mod dedup;
    mod identity;
    mod interrupted;
    mod radio;
    mod raw_streams;
    mod redial_storm;
    mod reorder;
    mod stats;
}
mod reconnect;
mod reorder;
mod reqrep;
mod sim;
#[cfg(unix)]
mod sim_unix;
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
    mod cli;
    mod drain;
    mod guide;
    mod identity_source;
    mod port_release;
    mod pubsub;
    mod reconnect;
    mod streams;
    mod tls_trust;
    mod transports;
    mod unsafe_guard;
}
