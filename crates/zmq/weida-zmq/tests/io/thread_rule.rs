//! libzmq's thread rule, asserted by the compiler.
//!
//! `zmq_socket(3)` §2: sockets "shall not be used by more than one thread
//! except after migrating a socket from one thread to another with a 'full
//! fence' memory barrier", and sharing one "results in undefined behaviour".
//! This crate answers that with the type system rather than with a document:
//! every socket type is `Send` and **not** `Sync`.
//!
//! Half of that is an ordinary assertion — `fn assert_send<T: Send>()`, which
//! the unit tests make. The other half is a negative, and there is no
//! negative bound to write, so it can only be asserted by compiling code that
//! must fail. That is what this harness is for:
//!
//! - `sharing_a_socket_is_rejected.rs` asks for `Sync` on every socket type
//!   and must not compile; its expected error is committed beside it.
//! - `moving_a_socket_into_a_task_compiles.rs` hands every socket type to a
//!   spawned future and must compile, so that the rule stays a rule about
//!   *sharing* and does not quietly become "a socket cannot leave its
//!   thread".
//!
//! Both files enumerate the socket types through
//! `weida_zmq::for_each_socket_type!`, which is the same list
//! `socket_endpoints!` requires membership of — so a socket type that arrives
//! without joining the list does not compile at all, and one that joins it is
//! checked here without anybody remembering to add it.
//!
//! **The plausible bug this fails on** is an `unsafe impl Sync` or a field
//! that makes `SocketCore`'s `PhantomData<Cell<()>>` moot: either would make
//! the compile-fail case compile, and this test would go red.
//!
//! Run with `TRYBUILD=overwrite` to refresh the expected error after a
//! deliberate change. The committed stderr quotes the `PhantomData`
//! declaration from `core/src/marker.rs`, which rustc can only print when
//! the `rust-src` component is installed — without it the same diagnostic
//! is three lines shorter and the comparison fails (measured on a Windows
//! runner). A gate host needs `rustup component add rust-src`.

#[test]
fn a_socket_is_send_and_not_sync() {
    let harness = trybuild::TestCases::new();
    harness.compile_fail("tests/thread_rule/sharing_a_socket_is_rejected.rs");
    harness.pass("tests/thread_rule/moving_a_socket_into_a_task_compiles.rs");
}
