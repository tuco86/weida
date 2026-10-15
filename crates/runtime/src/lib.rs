//! The reactor, the resolver and the OS plumbing a messaging library needs,
//! with no protocol in it.
//!
//! This crate is the answer to one question asked twice: *where does the
//! async runtime live?* A library that opens sockets needs tasks, timers and
//! name resolution, and its users must not have to be standing in a reactor
//! to call it. Everything here exists so that a library can own that reactor
//! instead of demanding one, and so that the same discipline does not have to
//! be written twice for two protocols
//! ([decisions/0013](../../../docs/decisions/0013-competitor-libraries.md)
//! §4.2).
//!
//! Nothing in this crate knows a wire format. It depends on `weida-core` for
//! one error vocabulary and on `tokio` for the reactor, and on nothing else;
//! it MUST NOT depend on `weida-protocol` or `weida`. A consumer with no
//! weida in the picture — a ZeroMQ or nanomsg implementation, say — is the
//! case every doc comment here is written for.
//!
//! # What is here
//!
//! - [`Exec`] — the *whole* surface onto the async runtime: [`Exec::spawn`],
//!   [`Exec::sleep`], [`Exec::within`], [`Exec::enter`] and
//!   [`Exec::resolve`]. A consumer holds one and never touches `tokio`
//!   directly, which is what makes "our library works on any executor" a
//!   checkable claim rather than an intention.
//! - Three ways to get one, differing only in where the reactor comes from:
//!   [`Exec::current`] borrows the ambient reactor, [`Exec::from_handle`]
//!   takes a handle to somebody else's, and [`Exec::owned`] creates one and
//!   hands back the [`OwnedReactor`] that owns it — with the
//!   background-shutdown discipline that dropping a reactor from inside its
//!   own worker thread requires.
//! - [`CloseBudget`] — a finite budget spanning the phases of a shutdown, so
//!   that no close waits on a peer's behaviour forever. ZeroMQ's
//!   `ZMQ_LINGER` defaults to infinite and `zmq_ctx_term()` can therefore
//!   block for as long as a peer likes; this type is what a finite default
//!   is built from.
//! - [`NameRegistry`] — a process- or context-scoped namespace of bound
//!   names with a byte budget, generic over what a bound name hands its
//!   acceptor. weida's `weida+inproc://` buses and ZeroMQ's `inproc://`
//!   endpoints are the same object under two names.
//! - On unix: `BoundUnixSocket`, which binds an `AF_UNIX` socket with the
//!   hygiene a filesystem endpoint needs — socket-type check, unlink before
//!   bind, an explicit mode rather than whatever `umask` allowed, a
//!   `sun_path` budget, and removal of the node on drop — and
//!   `peer_credentials`, the kernel's answer to *who is on the other end*
//!   ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
//!   §4.5).
//! - On Windows: `BoundPipe`, which creates the instances of a named pipe
//!   with an owner-only DACL, local clients only and the first-instance
//!   flag, `connect_pipe`, which opens the client end and waits out
//!   `ERROR_PIPE_BUSY`, and `client_principal` / `server_principal`, the
//!   kernel's answer to the same question in SID form (0010 §4.4, §4.5).
//!
//! # The grep
//!
//! `Exec` is the enforcement point, and the enforcement is mechanical. A grep
//! over a crate's own `src`:
//!
//! ```text
//! grep -rn 'tokio::spawn\|tokio::time\|lookup_host' crates/runtime/src # exec.rs and resolve.rs
//! ```
//!
//! A library built on this crate sends its tasks, timers and name lookups
//! through an `Exec`, and the same grep over its own `src` is the check for it
//! (`docs/ARCHITECTURE.md` §5). In this crate those names appear in
//! [`Exec`]'s own module and in the system resolver (`resolve.rs`), which
//! performs the one real lookup; that is the entire point of putting them
//! here.

mod budget;
mod exec;
#[cfg(windows)]
mod pipe;
mod registry;
mod resolve;
#[cfg(unix)]
mod unix;

pub use budget::CloseBudget;
pub use exec::{Exec, OwnedReactor};
#[cfg(windows)]
pub use pipe::{BoundPipe, client_principal, connect_pipe, server_principal};
pub use registry::NameRegistry;
pub use resolve::{MAX_OUTSTANDING_LOOKUPS, Resolved, Resolver, SharedResolver, SystemResolver};
#[cfg(unix)]
pub use unix::{BoundUnixSocket, peer_credentials};
