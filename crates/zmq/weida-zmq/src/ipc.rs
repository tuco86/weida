//! The `ipc` transport: `AF_UNIX`, its bind hygiene, and the kernel's answer
//! to *who is on the other end*.
//!
//! A filesystem endpoint is not a port. Every hazard that follows from that
//! has a documented answer, and the answers are not ZeroMQ's — they are the
//! same for any protocol with an `ipc://`-style transport, which is why they
//! live in `weida-runtime`'s [`BoundUnixSocket`] and
//! [`weida_runtime::peer_credentials`] and are used here
//! rather than written again
//! ([0010](../../../docs/decisions/0010-local-transport.md) §4.5,
//! `docs/research/ipc.md` §1.2, §1.5, §7).
//!
//! # Two path budgets, and which one applies where
//!
//! - **The endpoint string** is bounded by [`crate::MAX_IPC_ENDPOINT_BYTES`] where
//!   it is parsed: libzmq publishes "on Linux, the maximum is 113 characters
//!   including the `ipc://` prefix" (`zmq_ipc(7)`,
//!   `docs/research/zeromq.md` §11), and this library refuses at the same
//!   character so that a program ported from libzmq fails where it always
//!   did.
//! - **The path** is bounded by the kernel, at
//!   [`weida_core::MAX_SOCKET_PATH_BYTES`] — 107 bytes on Linux and 104 on
//!   macOS, `sun_path` minus its terminator — and that is checked by
//!   `BoundUnixSocket::bind`, because it is the number the kernel actually
//!   truncates at.
//!
//! On Linux the two coincide (107 + `"ipc://"`.len() == 113), which is why
//! libzmq's published number is really the kernel's. On macOS they do not:
//! `sun_path` is shorter there, so an endpoint that libzmq's documented limit
//! admits can still be refused by the bind, with the platform's own number in
//! the message. Both checks are kept rather than the stricter one alone,
//! because they answer different questions — "is this the endpoint libzmq
//! would have accepted" and "will the kernel store this path without
//! truncating it".
//!
//! # An endpoint is taken over, as libzmq's is
//!
//! `zmq_ipc(7)`: "if a second process binds to an endpoint already bound by a
//! process, this will succeed and the first process will lose its binding. In
//! this behaviour, the `ipc` transport is not consistent with the `tcp` or
//! `inproc` transports" (`docs/research/zeromq.md` §8). **This library
//! reproduces that**, because a program ported from libzmq may rely on it to
//! replace a running peer. A socket file outlives the process that created
//! it, so a bind has to replace a stale node or the endpoint would be
//! unbindable for good after a crash; here a node that something answers on
//! is replaced the same way ([`BoundUnixSocket::bind_replacing`]), and the
//! first binder keeps the connections it already accepted but receives no new
//! ones. `weida-runtime`'s plain `BoundUnixSocket::bind`, which `weida` and
//! `weida-nng` use, refuses a live listener instead.
//!
//! What else is done, and what is not:
//!
//! - **Only a socket is replaced.** Anything else at the path is refused
//!   (`EADDRINUSE`), because a bind that deletes a caller's file is worse
//!   than a bind that fails.
//! - **The node is bound in a private `0700` directory and renamed into
//!   place with mode `0600`**, never visible with a mode inherited from
//!   `umask`.
//! - **The node is removed on drop**, so the common case never meets the
//!   stale node at all. Only the node this binding created is removed, not
//!   the one of a binder that took the endpoint over.
//! - **The directory is load-bearing and this library cannot fix it.**
//!   Nothing inside the process can stop a process that may write the
//!   directory from removing the node and binding its own, which libzmq
//!   closes only with "unless directory ownership and permissions prevent
//!   endpoint substitution" (`docs/research/ipc.md` §1.2, §7). A caller
//!   putting a socket in a world-writable directory — `/tmp` — can have its
//!   endpoint substituted by any local user, and no check inside the process
//!   can prevent it. Put the socket in a directory you own and nobody else
//!   may write.
//!
//! # Credentials, not identity
//!
//! `SO_PEERCRED`/`LOCAL_PEERCRED` is captured once, when the connection is
//! made, and kept on the peer as a [`LocalPrincipal`]. It is the kernel's
//! statement about the process at the other end, which is why it can be
//! trusted at all, and it is *not* re-read per message: the values are a
//! snapshot of the peer as it was when the socket was created
//! (`docs/research/ipc.md` §1.5).
//!
//! Nothing in this crate authorizes anything with it. It is captured here
//! because an authorization handler needs it: libzmq's own
//! `ZMQ_IPC_FILTER_UID`/`_GID`/`_PID` did precisely this filtering and are
//! deprecated in favour of ZAP (`docs/research/zeromq.md` §10), so the
//! kernel's answer belongs on the peer and the decision belongs to a handler.
//! It is available as [`crate::Peer::credentials`].

use std::path::Path;

use tokio::net::{UnixListener, UnixStream};
use weida_core::LocalPrincipal;
use weida_runtime::{BoundUnixSocket, Exec, peer_credentials};

use crate::error::{Error, Result};
use crate::transport::Stream;

/// One bound `ipc://` endpoint: the listener, and the socket file until this
/// is dropped.
///
/// **Drop is the unlink**, as it is for `inproc`'s binding: the value lives in
/// the task that accepts on it, so `zmq_unbind` and closing a socket — which
/// abort that task — remove the node with no separate bookkeeping.
#[derive(Debug)]
pub struct IpcBinding {
    /// Removes the socket file when dropped.
    node: BoundUnixSocket,
    listener: UnixListener,
}

impl IpcBinding {
    /// Binds `path` with the hygiene above: the socket-type check, the
    /// private-directory bind and rename, the explicit mode, the kernel's
    /// path budget, and libzmq's takeover of a live endpoint.
    ///
    /// `exec` is entered while the listener is constructed, because tokio
    /// registers a socket with the reactor as it is created and the calling
    /// thread may have none of its own.
    pub fn bind(exec: &Exec, path: &Path) -> Result<IpcBinding> {
        let _guard = exec.enter();
        let (node, listener) = BoundUnixSocket::bind_replacing(path).map_err(ipc_error)?;
        Ok(IpcBinding { node, listener })
    }

    /// The path this endpoint is bound at.
    pub fn path(&self) -> &Path {
        self.node.path()
    }

    /// Accepts the next connection, with the peer's credentials captured at
    /// the moment the kernel reports them.
    pub async fn accept(&self) -> Result<Stream> {
        let (stream, _addr) = self.listener.accept().await?;
        Stream::unix(stream)
    }
}

/// Dials `path`.
///
/// The credentials of the process that bound the socket are captured here,
/// which is the dialling side's half of the same kernel fact: `SO_PEERCRED`
/// answers both ends.
pub async fn dial(path: &Path) -> Result<Stream> {
    let stream = UnixStream::connect(path).await?;
    Stream::unix(stream)
}

/// The credentials the kernel attributes to `stream`'s peer.
pub(crate) fn credentials(stream: &UnixStream) -> Result<LocalPrincipal> {
    peer_credentials(stream).map_err(ipc_error)
}

/// Maps `weida-core`'s vocabulary onto libzmq's errno names at the boundary,
/// keeping the path hazards recognisable: a refused path is `EADDRINUSE` when
/// something else holds it and `EINVAL` when it cannot be a socket path at
/// all.
fn ipc_error(error: weida_core::Error) -> Error {
    match error {
        weida_core::Error::InvalidAddress(why) if why.contains("is not a socket") => {
            Error::EADDRINUSE(
                format!("{why}; refusing to unlink a path that is not a socket").into(),
            )
        }
        other => Error::from(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{Context, ContextConfig};
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A socket path under the temporary directory.
    ///
    /// Honest for a test and wrong for a program: `/tmp` is world-writable,
    /// which is exactly the substitution hazard this module documents.
    fn scratch_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("weida-zmq-{}-{name}.sock", std::process::id()))
    }

    fn exec() -> Exec {
        Context::new(ContextConfig::default())
            .expect("context")
            .exec()
            .clone()
    }

    /// Claim: a bind creates the node with mode `0600` and removes it again
    /// when the binding is dropped — the hygiene of 0010 §4.5, observed on
    /// the filesystem rather than asserted in a comment.
    #[tokio::test]
    async fn a_bind_owns_the_node_and_its_mode() {
        let exec = exec();
        let path = scratch_path("hygiene");
        let binding = IpcBinding::bind(&exec, &path).expect("bind");
        let mode = std::fs::metadata(&path)
            .expect("the node exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the mode is set after bind, not inherited");
        assert_eq!(binding.path(), path.as_path());

        drop(binding);
        assert!(
            !path.exists(),
            "dropping the binding must remove the socket file"
        );
    }

    /// Claim: a path holding something that is not a socket is refused
    /// instead of unlinked — a bind that deletes a caller's file would be
    /// worse than a bind that fails.
    #[tokio::test]
    async fn a_path_that_is_not_a_socket_is_refused() {
        let exec = exec();
        let path = scratch_path("regular-file");
        std::fs::write(&path, b"not a socket").expect("write");

        let err = IpcBinding::bind(&exec, &path).unwrap_err();
        assert_eq!(err.errno(), "EADDRINUSE", "{err}");
        assert!(path.exists(), "the file must still be there");
        std::fs::remove_file(&path).expect("clean up");
    }

    /// Claim: a stale socket file — the node a crash leaves behind, with
    /// nobody listening — is replaced, because otherwise the endpoint would
    /// be permanently unbindable. A node somebody *is* listening on is taken
    /// over as libzmq does (`tests/ipc.rs`).
    #[tokio::test]
    async fn a_stale_node_is_replaced() {
        let exec = exec();
        let path = scratch_path("stale");
        // A listener that is closed and not unlinked leaves exactly what a
        // crash leaves: a socket node that nothing answers on.
        drop(std::os::unix::net::UnixListener::bind(&path).expect("bind"));
        assert!(path.exists());

        let second = IpcBinding::bind(&exec, &path).expect("rebind over the stale node");
        assert_eq!(second.path(), path.as_path());
    }

    /// Claim: a path past the kernel's `sun_path` budget is refused rather
    /// than silently truncated to a different endpoint.
    #[tokio::test]
    async fn an_over_long_path_is_refused_by_the_kernel_budget() {
        let exec = exec();
        let path = std::env::temp_dir().join("x".repeat(weida_core::MAX_SOCKET_PATH_BYTES));
        let err = IpcBinding::bind(&exec, &path).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
        assert!(
            err.cause().contains("sun_path"),
            "the message must name what refused it: {err}"
        );
    }

    /// Claim: both ends learn the other's credentials from the kernel, they
    /// agree because the peer is this process, and the pid is this process's
    /// where the platform reports one — the fact ZAP will be handed,
    /// captured at connect time.
    #[tokio::test]
    async fn both_ends_learn_the_peers_credentials() {
        let exec = exec();
        let path = scratch_path("credentials");
        let binding = IpcBinding::bind(&exec, &path).expect("bind");

        let dialling = tokio::spawn({
            let path = path.clone();
            async move { dial(&path).await }
        });
        let mut accepted = binding.accept().await.expect("accept");
        let mut dialled = dialling.await.expect("the task").expect("dial");

        let theirs = accepted.peer_credentials().expect("credentials at accept");
        let ours = dialled.peer_credentials().expect("credentials at connect");
        assert_eq!(
            theirs, ours,
            "both ends of one connection are this process, so the kernel says the same thing twice"
        );
        if let Some(pid) = theirs.pid {
            assert_eq!(pid, std::process::id(), "and it is this process");
        }

        // And it is a real connection underneath, not just a credential.
        dialled.write_all(b"hello").await.expect("write");
        let mut got = [0u8; 5];
        accepted.read_exact(&mut got).await.expect("read");
        assert_eq!(&got, b"hello");
    }

    /// Claim: a tcp or inproc stream has no credentials to report, because
    /// there is no kernel fact about a peer there — `None` rather than a
    /// zero that would read as root.
    #[tokio::test]
    async fn only_a_unix_stream_has_credentials() {
        let (theirs, _ours) = tokio::io::duplex(64);
        assert!(Stream::inproc(theirs).peer_credentials().is_none());
    }
}
