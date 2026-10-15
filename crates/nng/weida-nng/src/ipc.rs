//! The `ipc` transport: `AF_UNIX` with hygiene, and the kernel's word
//! about the peer.
//!
//! `nng_ipc(7)` "can expose OS-derived peer UID, GID, PID, and (where
//! applicable) zone ID; the UID/GID values are described as non-forgeable
//! at connection time" (`docs/research/nanomsg-nng.md` §10). NNG exposes
//! them as pipe options; here they arrive on the
//! [`PipeInfo`](crate::PipeInfo) a pipe-add-pre callback is handed, which
//! is where "local authorization logic" belongs — "this is application
//! policy, not SP authorization" (§10).
//!
//! **The PID is an observation, not a credential.** UID and GID are what
//! the kernel attributed to the peer at connection time and cannot be
//! forged; the PID is a number that identified a process *then* and may
//! identify a different one by the time anybody looks it up, so
//! authorizing on it is a time-of-check/time-of-use bug with a friendly
//! face ([0010](../../../docs/decisions/0010-local-transport.md) §4.4). It
//! is carried because a log line wants it and refused as a basis for a
//! decision by saying so here and at
//! [`weida_core::LocalPrincipal`].
//!
//! **The bind hygiene is `weida-runtime`'s and is not re-derived.**
//! [`BoundUnixSocket::bind`](weida_runtime::BoundUnixSocket::bind) does the
//! things a filesystem endpoint needs — the `sun_path` budget, the
//! socket-type check *before* anything is replaced, a probe that refuses a
//! node a live listener answers on (`EADDRINUSE`, as NNG's own `ipc://`
//! does), a bind in a private `0700` directory followed by a `rename` over a
//! stale node, and mode `0600` set before the node is visible rather than
//! whatever `umask` allowed — and removes the node when the binding drops.
//! NNG's `ipc://` has every one of those hazards; a caller of this transport
//! inherits the answers.
//!
//! **The directory is load-bearing.** Whoever can write the directory can
//! rename over the node afterwards, so the socket belongs in a directory this
//! process owns and no other user may write. That is a deployment property no
//! library can enforce, and it is said here rather than nowhere.

use std::path::Path;

use tokio::net::{UnixListener, UnixStream};
use weida_core::LocalPrincipal;
use weida_runtime::BoundUnixSocket;

use crate::error::{Error, Result};

/// One bound `ipc://` endpoint: the listener, and the node it removes when
/// it is dropped.
#[derive(Debug)]
pub struct IpcBinding {
    bound: BoundUnixSocket,
    listener: UnixListener,
}

impl IpcBinding {
    /// Binds `path` with `weida-runtime`'s hygiene.
    ///
    /// `NNG_EADDRINVAL` for a path past the platform's `sun_path` budget or
    /// occupied by something that is not a socket, `NNG_EADDRINUSE` when
    /// another listener holds it, `NNG_EPERM` when the directory says no.
    pub fn bind(path: &Path) -> Result<IpcBinding> {
        let (bound, listener) = BoundUnixSocket::bind(path).map_err(Error::from)?;
        Ok(IpcBinding { bound, listener })
    }

    /// The path this endpoint is bound at.
    pub fn path(&self) -> &Path {
        self.bound.path()
    }

    /// Accepts the next connection, with the kernel's statement about its
    /// peer taken now — which is the only moment it is true.
    pub async fn accept(&self) -> Result<(UnixStream, LocalPrincipal)> {
        let (stream, _) = self.listener.accept().await.map_err(Error::from)?;
        let principal = credentials(&stream)?;
        Ok((stream, principal))
    }
}

/// The credentials the kernel attributes to a connected peer.
///
/// Taken at connection time, because that is when the kernel's answer is
/// about the process that connected. UID and GID are non-forgeable; the
/// PID is an observation (see the module note).
pub fn credentials(stream: &UnixStream) -> Result<LocalPrincipal> {
    weida_runtime::peer_credentials(stream).map_err(Error::from)
}

/// Dials an `ipc://` endpoint.
pub async fn dial(path: &Path) -> Result<UnixStream> {
    UnixStream::connect(path).await.map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "weida-nng-{}-{}-{name}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        path
    }

    /// Claim: the bind sets `0600` explicitly rather than inheriting
    /// whatever `umask` allowed, and removes the node when the binding
    /// drops — the two halves of the hygiene a caller can see from
    /// outside.
    #[tokio::test]
    async fn a_bound_endpoint_is_private_and_leaves_nothing_behind() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_path("mode");
        let binding = IpcBinding::bind(&path).expect("bind");
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the mode is set after the bind, not inherited");
        drop(binding);
        assert!(
            !path.exists(),
            "the node is removed with the binding, so the next bind is not a stale-file race"
        );
    }

    /// Claim: a path occupied by something that is not a socket is refused
    /// rather than unlinked — removing it would delete a caller's data.
    #[tokio::test]
    async fn a_path_that_is_not_a_socket_is_refused() {
        let path = temp_path("file");
        std::fs::write(&path, b"not a socket").expect("write");
        let err = IpcBinding::bind(&path).unwrap_err();
        assert!(matches!(err, Error::EADDRINVAL(_)), "{err:?}");
        assert!(path.exists(), "and it is still there");
        std::fs::remove_file(&path).expect("cleanup");
    }

    /// Claim: the kernel's credentials reach the accepting side, and they
    /// are this process's own when this process is the peer.
    #[tokio::test]
    async fn the_kernel_names_the_peer() {
        let path = temp_path("creds");
        let binding = IpcBinding::bind(&path).expect("bind");
        let dialling = {
            let path = path.clone();
            tokio::spawn(async move { dial(&path).await })
        };
        let (_stream, principal) = binding.accept().await.expect("accept");
        dialling.await.expect("task").expect("dial");

        assert_eq!(principal.uid, unsafe_free_uid());
        assert_eq!(principal.pid, Some(std::process::id()));
    }

    /// This process's uid, without `unsafe`: the owner of a file it just
    /// created is itself.
    fn unsafe_free_uid() -> u32 {
        use std::os::unix::fs::MetadataExt;
        let path = temp_path("uid");
        std::fs::write(&path, b"").expect("write");
        let uid = std::fs::metadata(&path).expect("metadata").uid();
        std::fs::remove_file(&path).expect("cleanup");
        uid
    }
}
