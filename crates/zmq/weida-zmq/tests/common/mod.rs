//! The expected time of the suites that wait on a peer, and the wait itself.
//!
//! Each test file is its own crate and includes this module, so an item that
//! a file does not use would be dead code in that crate. `split.rs`,
//! `interop_zmqrs.rs` and `interop_libzmq.rs` all use everything here.

use std::time::Duration;

/// What one await takes on an idle machine, with room to spare. Past it the
/// wait reports and keeps waiting: a slow run is not a failed one.
pub const DEADLINE: Duration = Duration::from_secs(10);

/// Waits for `future`, reporting past [`DEADLINE`] instead of failing.
pub async fn within<F: Future>(future: F) -> F::Output {
    weida_testkit::within("the operation", DEADLINE, future).await
}
