//! The guide's claims, asserted against the guide's own programs.
//!
//! `docs/GUIDE.md` §1 makes five claims and `examples/guide_one_transfer.rs`
//! is the program that demonstrates each; §2 makes five more and
//! `examples/guide_many_peers.rs` is that chapter's program. This file drives
//! **those** files, included as modules, so what is asserted is the code a
//! reader runs and not a second copy of it written to be testable — the
//! discipline `crates/zmq/weida-zmq/tests/zguide_pirates.rs` already applies
//! to the zguide's recipes.
//!
//! One of chapter 1's claims is a **race**, and it is asserted as one: a
//! one-way transfer to a path nobody serves may be reported delivered,
//! because the FIN can be acknowledged before the dispatcher's refusal
//! travels back ([decisions/0005](../../../docs/decisions/0005-refusal-race.md)).
//! Pinning either side of that would be pinning a coin flip; what the test
//! pins is the guarantee that survives it — the answer is never
//! `Indeterminate`, and the same misroute as an exchange is definite.
//!
//! Chapter 2's claims are about a fan-out, where the same discipline applies
//! to **rates**: how many messages a stalled subscriber absorbs before the
//! first loss is a property of the connection on the day, so the tests assert
//! the *mechanism* — that nothing is dropped while the publisher is paced,
//! that something is once it is not, and which counter names it — and print
//! the numbers rather than pinning them.

use std::time::Duration;

use weida::Error;

#[path = "../examples/guide_one_transfer.rs"]
#[allow(dead_code)]
pub(crate) mod guide_one_transfer;

#[path = "../examples/guide_many_peers.rs"]
#[allow(dead_code)]
pub(crate) mod guide_many_peers;

#[path = "../examples/guide_late_is_lost.rs"]
#[allow(dead_code)]
pub(crate) mod guide_late_is_lost;

/// Generous ceiling: the streaming claim moves 64 MiB twice over loopback.
pub(crate) const DEADLINE: Duration = Duration::from_secs(60);

pub(crate) async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// The `Error` half of §1.3's vocabulary, without a network: the three
/// answers are distinct in the type system, not only in prose.
#[test]
fn the_three_answers_are_distinguishable_without_a_peer() {
    assert!(Error::UnknownEndpoint.is_definite_failure());
    assert!(Error::Rejected.is_definite_failure());
    assert!(
        !Error::Indeterminate.is_definite_failure(),
        "the whole point of `Indeterminate` is that it is not a failure"
    );
}
