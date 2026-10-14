//! The one-way protocols are one-way in the type system.
//!
//! "PUSH has no receive operation; SUB has no send operation" and "PULL
//! cannot send and PUSH cannot receive" (`docs/research/nanomsg-nng.md`
//! §4). A method that does not exist cannot be tested by calling it, so
//! this asserts the absence the only way an absence can be asserted: by
//! compiling code that must fail.
//!
//! **The plausible bug this fails on** is someone adding a `recv` to
//! `PushSocket` or a `send` to `PullSocket` for convenience — at which point
//! NNG's rule would have become a comment, and this test goes red.
//!
//! Run with `TRYBUILD=overwrite` to refresh the expected errors after a
//! deliberate change.

#[test]
fn the_one_way_protocols_have_only_their_one_way() {
    let harness = trybuild::TestCases::new();
    harness.compile_fail("tests/directions/a_pusher_cannot_receive.rs");
    harness.compile_fail("tests/directions/a_puller_cannot_send.rs");
    harness.pass("tests/directions/each_direction_that_exists_compiles.rs");
}
