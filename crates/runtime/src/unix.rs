//! `AF_UNIX` bind hygiene and the kernel's answer to *who is on the other
//! end*.
//!
//! A filesystem endpoint is not a port: it has an owner, a mode, a path
//! length the kernel truncates at, and a node that outlives the process that
//! bound it. Every one of those is a hazard with a known answer, and the
//! answers are the same for any protocol that offers an `ipc://`-style
//! transport — weida's `weida+unix://` and ZeroMQ's `ipc://` differ in what
//! they write on the socket, not in how they bind it
//! ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.5,
//! `docs/research/ipc.md` §1.2, §1.5, §7).

use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use tokio::net::{UnixListener, UnixStream};
use weida_core::{Error, LocalPrincipal, MAX_SOCKET_PATH_BYTES};

/// Mode of a bound socket file.
///
/// Set explicitly after bind, never inherited: a new socket file gets every
/// permission bit `umask` does not mask, so the default is whatever the
/// process happened to inherit (`docs/research/ipc.md` §1.2, [0010 §4.5]).
const SOCKET_MODE: u32 = 0o600;

/// Mode of the private directory a socket is bound in before it is moved into
/// place: nobody but the owner can reach a node inside it, whatever mode the
/// node has.
const STAGING_MODE: u32 = 0o700;

/// Distinguishes the staging directories of one process.
static STAGING: AtomicU64 = AtomicU64::new(0);

/// A bound `AF_UNIX` socket file: the node exists as long as this value
/// does.
///
/// Hold it beside the [`UnixListener`] it was bound with. Dropping it removes
/// the node it bound — and only that node — so the *next* bind of the same
/// path finds nothing to replace.
#[derive(Debug)]
pub struct BoundUnixSocket {
    path: PathBuf,
    /// Device and inode of the node this value bound, so that dropping it
    /// removes that node and not whatever has taken the path since.
    node: (u64, u64),
}

impl BoundUnixSocket {
    /// Binds `path` with the hygiene a filesystem endpoint needs, replacing a
    /// stale socket file left by a crash.
    ///
    /// Four things happen here, and each answers a documented hazard:
    ///
    /// 1. **The path budget.** `sun_path` is 108 bytes on Linux and 104 on
    ///    macOS *including* the terminator, and the kernel truncates rather
    ///    than failing, so an over-long path binds something other than what
    ///    was asked for. It is refused instead
    ///    (`docs/research/ipc.md` §1.1, §2.1). libzmq's `ipc://` publishes
    ///    the same limit as "113 characters including the prefix" and leaves
    ///    the rest to the caller (`docs/research/zeromq.md` §11).
    /// 2. **The socket-type check, and a probe.** Closing a socket does not
    ///    remove its node, so a crash leaves one and `bind()` then fails with
    ///    `EADDRINUSE`. A stale node is a socket nobody is listening on:
    ///    anything else at that path is not ours to remove — removing it is
    ///    how a bind deletes a caller's data — and neither is a socket
    ///    somebody *is* listening on, which is found out by connecting to it:
    ///    a live listener answers, and the bind fails with
    ///    [`ErrorKind::AddrInUse`] instead of silently taking its endpoint.
    /// 3. **Bind in private, then move into place.** The socket is bound
    ///    inside a `0700` directory next to the final path, given its mode
    ///    there, and `rename`d to the final path, which replaces a stale node
    ///    in one step. No other user can reach the node while it is bound and
    ///    before it is `0600`, and the path is never absent between unlinking
    ///    and binding — the substitution race of
    ///    `docs/research/ipc.md` §1.2, §7 has no window to use. **The
    ///    directory is still load-bearing**: the caller MUST place the socket
    ///    in a directory it owns and that no other user may write, because
    ///    whoever can write there can rename over the node afterwards. libzmq's
    ///    `ipc://` has this hazard too and answers none of it. A path too close
    ///    to the budget to leave its `sun_path` room for the private directory
    ///    binds in place and gets its mode after, which is the window the
    ///    private directory closes.
    /// 4. **The mode, explicitly.** `0600`, because a socket file is created
    ///    with whatever `umask` allows, which is whatever the process
    ///    happened to inherit.
    pub fn bind(path: &Path) -> Result<(BoundUnixSocket, UnixListener), Error> {
        Self::bind_node(path, false)
    }

    /// Binds `path` like [`bind`](Self::bind), but takes the endpoint over
    /// from a live listener instead of refusing it.
    ///
    /// This is libzmq's `ipc://` behaviour: "if a second process binds to an
    /// endpoint already bound by a process, this will succeed and the first
    /// process will lose its binding" (`docs/research/zeromq.md` §8). The
    /// first listener keeps the connections it has accepted and stops
    /// receiving new ones, because the path now names the new node. Everything
    /// else is as in [`bind`](Self::bind): the node is built in the private
    /// directory and `rename`d over the old one, so it is never visible with a
    /// mode `umask` chose, and anything that is not a socket is still refused.
    ///
    /// Only a protocol whose own semantics are "the last binder wins" should
    /// call this. Anyone who can write the directory can silence a service
    /// with it, which is the directory hazard of [`bind`](Self::bind) made
    /// cheaper: no probe stands in the way.
    pub fn bind_replacing(path: &Path) -> Result<(BoundUnixSocket, UnixListener), Error> {
        Self::bind_node(path, true)
    }

    fn bind_node(
        path: &Path,
        replace_live: bool,
    ) -> Result<(BoundUnixSocket, UnixListener), Error> {
        if path.as_os_str().len() > MAX_SOCKET_PATH_BYTES {
            return Err(Error::InvalidAddress(format!(
                "socket path exceeds this platform's {}-byte sun_path budget: {}",
                MAX_SOCKET_PATH_BYTES,
                path.display()
            )));
        }
        // A stale node is a socket nobody is listening on. Anything else at
        // that path is not ours to remove.
        let stale = match std::fs::symlink_metadata(path) {
            Ok(meta) if is_socket(&meta) => {
                if !replace_live && answers(path) {
                    return Err(Error::Io(std::io::Error::new(
                        ErrorKind::AddrInUse,
                        format!("{} has a listener", path.display()),
                    )));
                }
                true
            }
            Ok(_) => {
                return Err(Error::InvalidAddress(format!(
                    "{} exists and is not a socket",
                    path.display()
                )));
            }
            Err(_) => false,
        };
        let listener = match Staging::create(path)? {
            Some(staging) => {
                let listener = UnixListener::bind(&staging.socket).map_err(Error::Io)?;
                std::fs::set_permissions(
                    &staging.socket,
                    std::fs::Permissions::from_mode(SOCKET_MODE),
                )
                .map_err(Error::Io)?;
                std::fs::rename(&staging.socket, path).map_err(Error::Io)?;
                listener
            }
            None => {
                if stale {
                    std::fs::remove_file(path).map_err(Error::Io)?;
                }
                let listener = UnixListener::bind(path).map_err(Error::Io)?;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
                    .map_err(Error::Io)?;
                listener
            }
        };
        let node = std::fs::symlink_metadata(path).map_err(Error::Io)?;
        Ok((
            BoundUnixSocket {
                path: path.to_path_buf(),
                node: (node.dev(), node.ino()),
            },
            listener,
        ))
    }

    /// The path this socket is bound at.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BoundUnixSocket {
    fn drop(&mut self) {
        // Leaving the node behind is what forces the next bind to replace a
        // stale node; removing it on the way out keeps the common case
        // free of that replacement. Only the node this value bound: a successor that
        // replaced it has a node of its own at the same path, and unlinking by
        // path would delete the successor's endpoint.
        let Ok(meta) = std::fs::symlink_metadata(&self.path) else {
            return;
        };
        if is_socket(&meta) && (meta.dev(), meta.ino()) == self.node {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// A private directory beside the final path, holding the socket until it is
/// ready to be moved into place.
struct Staging {
    dir: PathBuf,
    socket: PathBuf,
}

impl Staging {
    /// Creates the directory, or `None` when the socket's path inside it
    /// would be over the `sun_path` budget.
    fn create(path: &Path) -> Result<Option<Staging>, Error> {
        let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
            return Ok(None);
        };
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        // The process id and a counter: short, and unique among this
        // process's binds. A leftover of a crashed process with the same id is
        // skipped over rather than reused.
        for _ in 0..8 {
            let dir = parent.join(format!(
                ".w{:x}{:x}",
                std::process::id(),
                STAGING.fetch_add(1, Ordering::Relaxed)
            ));
            let socket = dir.join(name);
            if socket.as_os_str().len() > MAX_SOCKET_PATH_BYTES {
                return Ok(None);
            }
            match std::fs::DirBuilder::new().mode(STAGING_MODE).create(&dir) {
                Ok(()) => return Ok(Some(Staging { dir, socket })),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(Error::Io(e)),
            }
        }
        Err(Error::Io(std::io::Error::new(
            ErrorKind::AlreadyExists,
            "no free staging directory name",
        )))
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        // What is left when a bind failed half way, or nothing when it
        // succeeded: the socket has been moved out by then.
        let _ = std::fs::remove_file(&self.socket);
        let _ = std::fs::remove_dir(&self.dir);
    }
}

/// Does anything answer on the socket node at `path`?
///
/// One poll of a non-blocking connect settles it. A node nobody listens on
/// refuses the connection inside that poll; a listener accepts it, which shows
/// as a connection still being set up (or, with its backlog full, as an
/// error that is not a refusal). Anything that cannot be told apart from a
/// listener is treated as one: the cost of a wrong answer is a bind that
/// fails, against an endpoint taken from a process that was using it. The
/// connection is dropped at once, and the listener it reached sees a peer
/// that said nothing.
fn answers(path: &Path) -> bool {
    let connect = UnixStream::connect(path);
    let mut connect = std::pin::pin!(connect);
    match connect
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Err(e)) => {
            !matches!(e.kind(), ErrorKind::ConnectionRefused | ErrorKind::NotFound)
        }
        Poll::Ready(Ok(_)) | Poll::Pending => true,
    }
}

fn is_socket(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::FileTypeExt;
    meta.file_type().is_socket()
}

/// The credentials the kernel attributes to the peer of `stream`.
///
/// This is the whole of a local peer's identity: there is no key, no
/// certificate and nothing the peer asserts about itself — the kernel says
/// what it is, which is why it can be trusted at all
/// ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.4,
/// §4.5).
///
/// Taken at accept or connect time, which is when `SO_PEERCRED` captures
/// them; they are **not** re-read per message, and re-reading would not help:
/// the values are a snapshot of the peer as it was when the socket was
/// created (`docs/research/ipc.md` §1.5). The pid is an `Option` because
/// macOS's `LOCAL_PEERCRED` reports none, and because a pid is an
/// observation — it may be reused — rather than an identity.
///
/// A protocol that uses this as an authorization input should say so
/// explicitly: libzmq's `ZMQ_IPC_FILTER_UID`/`_GID`/`_PID` did exactly this
/// and are deprecated in favour of ZAP, which is a decision about *where*
/// authorization lives and not about whether the kernel's answer is true
/// (`docs/research/zeromq.md` §10).
pub fn peer_credentials(stream: &UnixStream) -> Result<LocalPrincipal, Error> {
    let cred = stream.peer_cred().map_err(Error::Io)?;
    Ok(LocalPrincipal {
        uid: cred.uid(),
        gid: cred.gid(),
        // macOS reports no PID at all, and a PID is an observation even where
        // it exists [0010 §4.4].
        pid: cred.pid().map(|pid| pid as u32),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A private directory per test, which is also what the substitution race
    /// of `bind` requires of a caller.
    fn dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("weida-runtime-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test directory");
        dir
    }

    /// This process's uid without `unsafe`: the effective uid of the owner of
    /// a file this process just created.
    fn unsafe_free_uid() -> u32 {
        use std::os::unix::fs::MetadataExt;
        let path = dir("uid").join("owned");
        std::fs::write(&path, b"").expect("write");
        let uid = std::fs::metadata(&path).expect("metadata").uid();
        let _ = std::fs::remove_file(&path);
        uid
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: the mode is what this function sets, not what `umask` allowed.
        #[tokio::test]
        async fn a_bound_socket_is_private_to_its_owner() {
            let path = dir("mode").join("s");
            let (bound, _listener) = BoundUnixSocket::bind(&path).expect("bind");
            let mode = std::fs::metadata(&path)
                .expect("metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, SOCKET_MODE, "mode {mode:o}");
            assert_eq!(bound.path(), path.as_path());
        }
        /// Claim: the node goes away with the binding, so the next bind of the
        /// same path has nothing to replace.
        #[tokio::test]
        async fn the_node_is_removed_on_drop() {
            let path = dir("drop").join("s");
            let (bound, listener) = BoundUnixSocket::bind(&path).expect("bind");
            assert!(path.exists());
            drop(listener);
            drop(bound);
            assert!(!path.exists(), "the socket node outlived its binding");
        }
        /// Claim: a stale node — a socket file with nobody listening, which is
        /// what a crash leaves — is replaced rather than reported as
        /// `EADDRINUSE`.
        #[tokio::test]
        async fn a_stale_socket_is_replaced() {
            let path = dir("stale").join("s");
            {
                let (bound, listener) = BoundUnixSocket::bind(&path).expect("first bind");
                // Forget the guard the way a crash does: the node stays behind.
                std::mem::forget(bound);
                drop(listener);
            }
            assert!(path.exists(), "the stale node must still be there");
            let (_bound, _listener) =
                BoundUnixSocket::bind(&path).expect("bind over the stale node");
        }
        /// Claim: a socket somebody is listening on is not taken. The second bind
        /// fails, and the first listener still answers afterwards.
        #[tokio::test]
        async fn a_live_socket_is_not_taken() {
            let path = dir("live").join("s");
            let (_bound, listener) = BoundUnixSocket::bind(&path).expect("first bind");

            let err = BoundUnixSocket::bind(&path).unwrap_err();
            assert!(
                matches!(&err, Error::Io(e) if e.kind() == ErrorKind::AddrInUse),
                "{err:?}"
            );

            let accepting = tokio::spawn(async move { listener.accept().await.map(|_| ()) });
            UnixStream::connect(&path)
                .await
                .expect("the first listener still owns the path");
            weida_testkit::within(
                "the first listener to accept",
                std::time::Duration::from_secs(5),
                accepting,
            )
            .await
            .expect("the accept task ran")
            .expect("an accepted connection");
        }
        /// Claim: `bind_replacing` takes the path from a live listener: new
        /// connections reach the second binder, in mode `0600`, and dropping the
        /// first guard afterwards leaves the second's node alone.
        #[tokio::test]
        async fn bind_replacing_takes_a_live_socket_over() {
            let path = dir("takeover").join("s");
            let (first, _first_listener) = BoundUnixSocket::bind(&path).expect("first bind");
            let (_second, second_listener) =
                BoundUnixSocket::bind_replacing(&path).expect("takes the live node");
            let mode = std::fs::metadata(&path)
                .expect("metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, SOCKET_MODE, "mode {mode:o}");

            drop(first);
            let accepting = tokio::spawn(async move { second_listener.accept().await.map(|_| ()) });
            UnixStream::connect(&path)
                .await
                .expect("the second binder owns the path");
            weida_testkit::within(
                "the second binder to accept",
                std::time::Duration::from_secs(5),
                accepting,
            )
            .await
            .expect("the accept task ran")
            .expect("an accepted connection");
        }
        /// Claim: taking a live node over still leaves nothing behind, still
        /// refuses what is not a socket, and still honours the path budget.
        #[tokio::test]
        async fn bind_replacing_keeps_the_other_hygiene() {
            let dir = dir("takeover-hygiene");
            let path = dir.join("s");
            let (_first, _first_listener) = BoundUnixSocket::bind(&path).expect("first bind");
            let (_second, _second_listener) =
                BoundUnixSocket::bind_replacing(&path).expect("takeover");
            let names: Vec<_> = std::fs::read_dir(&dir)
                .expect("directory")
                .map(|entry| entry.expect("entry").file_name())
                .collect();
            assert_eq!(names, [std::ffi::OsString::from("s")]);

            let file = dir.join("f");
            std::fs::write(&file, b"not a socket").expect("write");
            let err = BoundUnixSocket::bind_replacing(&file).unwrap_err();
            assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
            assert_eq!(std::fs::read(&file).expect("read"), b"not a socket");

            let long = dir.join("x".repeat(MAX_SOCKET_PATH_BYTES + 1));
            let err = BoundUnixSocket::bind_replacing(&long).unwrap_err();
            assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
        }
        /// Claim: replacing a live node never shows the path in a mode `umask`
        /// chose, nor absent: the node is renamed over, not unlinked first.
        #[tokio::test]
        async fn a_takeover_is_never_visible_in_the_mode_umask_gave_it() {
            use std::sync::Arc;
            use std::sync::atomic::AtomicBool;

            const TAKEOVERS: usize = 200;
            let path = dir("takeover-window").join("s");
            let mut held = vec![BoundUnixSocket::bind(&path).expect("first bind")];
            let done = Arc::new(AtomicBool::new(false));
            let watcher = {
                let (path, done) = (path.clone(), Arc::clone(&done));
                std::thread::spawn(move || {
                    let mut bad = Vec::new();
                    while !done.load(Ordering::Relaxed) {
                        match std::fs::symlink_metadata(&path) {
                            Ok(meta) if meta.permissions().mode() & 0o777 != SOCKET_MODE => {
                                bad.push(format!("mode {:o}", meta.permissions().mode() & 0o777));
                            }
                            Ok(_) => {}
                            Err(_) => bad.push("absent".to_owned()),
                        }
                    }
                    bad
                })
            };
            for _ in 0..TAKEOVERS {
                held.push(BoundUnixSocket::bind_replacing(&path).expect("takeover"));
            }
            done.store(true, Ordering::Relaxed);
            let bad = watcher.join().expect("the watcher ran");
            assert!(bad.is_empty(), "the path was not 0600 and present: {bad:?}");
        }
        /// Claim: a guard removes the node it bound and not the one that replaced
        /// it. The first listener is gone, so its node is stale and the second
        /// bind takes the path; dropping the first guard afterwards must leave the
        /// second's endpoint alone.
        #[tokio::test]
        async fn dropping_a_guard_leaves_the_node_of_its_successor() {
            let path = dir("successor").join("s");
            let (first, first_listener) = BoundUnixSocket::bind(&path).expect("first bind");
            drop(first_listener);
            let (_second, second_listener) =
                BoundUnixSocket::bind(&path).expect("takes the stale node");

            drop(first);
            assert!(
                path.exists(),
                "dropping the first guard removed the second one's node"
            );
            let accepting = tokio::spawn(async move { second_listener.accept().await.map(|_| ()) });
            UnixStream::connect(&path)
                .await
                .expect("the successor still owns the path");
            weida_testkit::within(
                "the successor to accept",
                std::time::Duration::from_secs(5),
                accepting,
            )
            .await
            .expect("the accept task ran")
            .expect("an accepted connection");
        }
        /// Claim: no one can find the node in a mode `umask` chose while it is
        /// being bound. A reader polls the paths that are about to be bound for
        /// the whole run; it must never see one that is not `0600`.
        #[tokio::test]
        async fn a_node_is_never_visible_in_the_mode_umask_gave_it() {
            use std::sync::Arc;
            use std::sync::atomic::AtomicBool;

            const BINDS: usize = 200;
            let dir = dir("window");
            let paths: Vec<PathBuf> = (0..BINDS).map(|n| dir.join(format!("s{n}"))).collect();
            let done = Arc::new(AtomicBool::new(false));
            let watcher = {
                let (paths, done) = (paths.clone(), Arc::clone(&done));
                std::thread::spawn(move || {
                    let mut loose = Vec::new();
                    while !done.load(Ordering::Relaxed) {
                        for path in &paths {
                            if let Ok(meta) = std::fs::symlink_metadata(path)
                                && meta.permissions().mode() & 0o777 != SOCKET_MODE
                            {
                                loose.push((path.clone(), meta.permissions().mode() & 0o777));
                            }
                        }
                    }
                    loose
                })
            };
            let mut bound = Vec::new();
            for path in &paths {
                bound.push(BoundUnixSocket::bind(path).expect("bind"));
            }
            done.store(true, Ordering::Relaxed);
            let loose = watcher.join().expect("the watcher ran");
            assert!(
                loose.is_empty(),
                "a socket was visible in a mode other than {SOCKET_MODE:o}: {loose:?}"
            );
        }
        /// Claim: binding leaves nothing behind but the socket, and the private
        /// directory it is bound in does not outlive the bind.
        #[tokio::test]
        async fn binding_leaves_nothing_but_the_socket() {
            let dir = dir("clean");
            let path = dir.join("s");
            let (_bound, _listener) = BoundUnixSocket::bind(&path).expect("bind");
            let names: Vec<_> = std::fs::read_dir(&dir)
                .expect("directory")
                .map(|entry| entry.expect("entry").file_name())
                .collect();
            assert_eq!(names, [std::ffi::OsString::from("s")]);
        }
        /// Claim: a path that fits the budget but leaves no room for the private
        /// directory still binds, with the same mode and the same protection of a
        /// live listener.
        #[tokio::test]
        async fn a_path_with_no_room_to_stage_still_binds() {
            let base = dir("tight");
            let name = "x".repeat(MAX_SOCKET_PATH_BYTES - base.as_os_str().len() - 1);
            let path = base.join(name);
            assert_eq!(path.as_os_str().len(), MAX_SOCKET_PATH_BYTES);

            let (_bound, _listener) = BoundUnixSocket::bind(&path).expect("bind at the budget");
            let mode = std::fs::metadata(&path)
                .expect("metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, SOCKET_MODE, "mode {mode:o}");
            let err = BoundUnixSocket::bind(&path).unwrap_err();
            assert!(
                matches!(&err, Error::Io(e) if e.kind() == ErrorKind::AddrInUse),
                "{err:?}"
            );
        }
        /// Claim: anything at that path that is not a socket is refused, because
        /// unlinking it would delete somebody's file.
        #[tokio::test]
        async fn a_regular_file_is_not_unlinked() {
            let path = dir("file").join("s");
            std::fs::write(&path, b"not a socket").expect("write");
            let err = BoundUnixSocket::bind(&path).unwrap_err();
            assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
            assert_eq!(std::fs::read(&path).expect("read"), b"not a socket");
        }
        /// Claim: a path the kernel would truncate is refused before it binds
        /// something other than what was asked for.
        #[tokio::test]
        async fn an_over_long_path_is_refused() {
            let path = dir("long").join("x".repeat(MAX_SOCKET_PATH_BYTES + 1));
            let err = BoundUnixSocket::bind(&path).unwrap_err();
            assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
        }
        /// Claim: the credentials are the kernel's, and on a socket between two
        /// halves of this process they are this process's own.
        #[tokio::test]
        async fn peer_credentials_are_the_kernels_answer() {
            let (a, _b) = UnixStream::pair().expect("socket pair");
            let principal = peer_credentials(&a).expect("credentials");
            assert_eq!(principal.uid, unsafe_free_uid());
            if let Some(pid) = principal.pid {
                assert_eq!(pid, std::process::id());
            }
        }
    }
}
