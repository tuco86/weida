//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate's dependencies
//! are linked once per configuration instead of once per file.

mod common;

mod accept_limits;
mod alpn;
mod blocking;
mod bus;
mod cli;
mod cursors;
mod dedup;
mod drain;
mod flows;
mod foreign_executor;
mod guide;
mod hostile;
mod identity;
mod identity_source;
mod interrupted;
mod local_hostile;
mod pair;
mod pubsub;
mod pushpull;
mod radio;
mod raw_streams;
mod reconnect;
mod redial_storm;
mod reorder;
mod reqrep;
#[cfg(feature = "test-util")]
mod sim;
mod stats;
mod streams;
mod survey;
#[cfg(feature = "swarm")]
mod swarm;
mod tls_trust;
mod transports;
mod unsafe_guard;
