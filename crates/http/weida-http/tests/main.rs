//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate and its peers are
//! linked once per configuration instead of once per file.
//!
//! Everything here opens loopback sockets, so it all lives under `mod io`, the
//! integration tier of `.config/nextest.toml`.

mod common;

mod io {
    // Every row has a TLS twin, and the client's TLS option exists only
    // with the feature.
    #[cfg(feature = "tls")]
    mod interop_hyper;
}
