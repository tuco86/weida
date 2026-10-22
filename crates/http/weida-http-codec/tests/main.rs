//! Every integration test of this crate in one binary: each file below is a
//! module rather than a test target of its own, so the crate is linked once
//! per configuration instead of once per file.

mod fuzz_smoke;
mod golden_h2;
mod golden_hpack;
mod golden_vectors;
