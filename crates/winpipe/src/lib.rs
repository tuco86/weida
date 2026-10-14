//! The Win32 calls a named-pipe transport needs, behind a safe surface.
//!
//! Most crates in this workspace forbid `unsafe`. A named pipe cannot be
//! made private, and its peer cannot be identified, without four things
//! that `std` and `tokio` do not expose: a security descriptor on
//! `CreateNamedPipe`, `ImpersonateNamedPipeClient` followed by a read of
//! the impersonation token's user SID, the pipe object's owner SID for the
//! client side, and the two process ids
//! ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.4,
//! §4.5; `docs/research/ipc.md` §3.2, §3.3). This crate is those four
//! things and nothing else, so that the `unsafe` a platform demands is in
//! one place a reviewer can read in one sitting, and so that every consumer
//! — `weida-runtime` — keeps its own `unsafe_code = "forbid"`.
//!
//! Nothing here knows a protocol, an address form or a peer: it takes and
//! returns `tokio`'s pipe handles, strings and integers.
//!
//! On every platform but Windows the crate is empty.

#[cfg(windows)]
mod imp;

#[cfg(windows)]
pub use imp::{
    OwnerOnlyDacl, PipePeer, client_peer, create_instance, is_pipe_busy, open_client, server_peer,
};
