//! The bound the suites that wait on a peer put on every await.
//!
//! Each test file is its own crate and includes this module, so an item that
//! a file does not use would be dead code in that crate. `split.rs`,
//! `interop_zmqrs.rs` and `interop_libzmq.rs` all use everything here.

use std::time::Duration;

/// Long enough that a loaded machine is not a failure, short enough that a
/// hang is not a hung suite.
pub const DEADLINE: Duration = Duration::from_secs(10);

/// Bounds `future` by [`DEADLINE`], so a wrong turn fails the test rather than
/// hanging the suite.
pub async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("the operation timed out")
}
