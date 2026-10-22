//! Named-pipe hygiene and the kernel's answer to *who is on the other end*,
//! on Windows.
//!
//! A pipe name is not a port either: the default security descriptor lets
//! Everyone read, the name can be squatted by whoever creates it first, a
//! server instance is a one-shot object that has to be recreated after every
//! accept, and a client that finds every instance taken gets
//! `ERROR_PIPE_BUSY` rather than a queue. Each of those has a known answer,
//! and the answers are the same for any protocol with an `ipc://`-style
//! transport ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
//! §4.5, `docs/research/ipc.md` §3.1-§3.3).
//!
//! **Byte mode, not message mode.** A connection carries one stream whose
//! frames are the protocol's own
//! ([0012](../../../docs/decisions/0012-local-connection-grouping.md)), so the
//! kernel framing of message mode ([0010](../../../docs/decisions/0010-local-transport.md)
//! §4.5) would go unused — and `mio` reads a message-mode pipe into a fixed
//! buffer, where a message longer than the buffer is `ERROR_MORE_DATA` and a
//! read error rather than a partial read. The end of a stream, which `AF_UNIX`
//! signals by half-close and a pipe cannot, is the consumer's to mark.

use std::ffi::OsString;
use std::time::Duration;

use tokio::net::windows::named_pipe::{NamedPipeClient, NamedPipeServer};
use weida_core::{Error, LossCause, WindowsPrincipal};

use crate::Exec;

/// The first wait before a client opens a busy pipe again; each further one
/// doubles, up to [`BUSY_STEP_MAX`].
///
/// Busy means every instance is connected and the server has not created
/// the next one: for one accept-loop iteration, or for as long as the
/// server holds back accepting because its places are taken. The second is
/// backpressure, the same wait an `AF_UNIX` client spends in the listen
/// backlog, so there is no budget here: the caller's own deadline ends it.
const BUSY_STEP: Duration = Duration::from_millis(10);

/// The longest wait between two opens of a busy pipe: a client stuck behind
/// a full server tries five times a second rather than a hundred.
const BUSY_STEP_MAX: Duration = Duration::from_millis(200);

/// A named pipe this process owns: the name is held as long as at least one
/// instance exists, and every instance carries the same descriptor.
///
/// Hold it beside the accept loop. The first instance is created with
/// `FILE_FLAG_FIRST_PIPE_INSTANCE`, so a name that already exists —
/// another process's pipe, or a squatter waiting for our clients — is a
/// bind failure rather than a silent share.
#[derive(Debug)]
pub struct BoundPipe {
    path: OsString,
    dacl: weida_winpipe::OwnerOnlyDacl,
}

impl BoundPipe {
    /// Creates the pipe at `path` (`\\.\pipe\<name>`) with its first
    /// listening instance.
    ///
    /// Three things happen here, each answering a documented hazard:
    ///
    /// 1. **The descriptor, explicitly.** The default grants read access to
    ///    Everyone and the anonymous account; this one grants full access to
    ///    the account this process runs as and to `SYSTEM`, and nothing to
    ///    anyone else (`docs/research/ipc.md` §3.2).
    /// 2. **Local clients only.** `PIPE_REJECT_REMOTE_CLIENTS` on every
    ///    instance; the address form's `\\.\pipe\` prefix is the other half
    ///    of the same rule [0010 §4.8].
    /// 3. **First instance or nothing.** A name that exists is not ours.
    pub fn bind(path: impl Into<OsString>) -> Result<(BoundPipe, NamedPipeServer), Error> {
        let path = path.into();
        let dacl = weida_winpipe::OwnerOnlyDacl::for_current_user().map_err(Error::Io)?;
        let first = weida_winpipe::create_instance(&path, &dacl, true).map_err(|e| {
            if e.kind() == std::io::ErrorKind::PermissionDenied {
                Error::InvalidAddress(format!(
                    "{} already exists: a pipe name that is taken is not ours to serve",
                    path.to_string_lossy()
                ))
            } else {
                Error::Io(e)
            }
        })?;
        Ok((BoundPipe { path, dacl }, first))
    }

    /// The next listening instance, created after each accept so that a
    /// client never finds no instance at all.
    pub fn next_instance(&self) -> Result<NamedPipeServer, Error> {
        weida_winpipe::create_instance(&self.path, &self.dacl, false).map_err(Error::Io)
    }

    /// The pipe's OS path.
    pub fn path(&self) -> &std::ffi::OsStr {
        &self.path
    }
}

/// Opens the client end of the pipe at `path`, waiting out
/// `ERROR_PIPE_BUSY`.
///
/// Busy is either the accept-loop race of `docs/research/ipc.md` §3.1, every
/// instance connected and the next one not created yet, or a server that
/// holds back accepting while its places are taken. Rather than
/// `WaitNamedPipe`, which blocks a thread, the open is retried on the
/// runtime's timer, from 10 ms doubling to 200 ms between tries, for as
/// long as the pipe stays busy: the wait is backpressure and ends when the
/// caller stops waiting. A pipe that does not exist is the peer being gone,
/// reported as such so a caller can redial.
pub async fn connect_pipe(exec: &Exec, path: &std::ffi::OsStr) -> Result<NamedPipeClient, Error> {
    let mut step = BUSY_STEP;
    loop {
        let attempt = {
            // Inside the runtime context: tokio registers the handle with the
            // reactor as it is opened.
            let _guard = exec.enter();
            weida_winpipe::open_client(path)
        };
        match attempt {
            Ok(client) => return Ok(client),
            Err(e) if weida_winpipe::is_pipe_busy(&e) => {
                exec.sleep(step).await;
                step = (step * 2).min(BUSY_STEP_MAX);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::ConnectionLost(LossCause::PeerClosed));
            }
            Err(e) => return Err(Error::Io(e)),
        }
    }
}

/// The principal the kernel attributes to the client of `server`.
///
/// Callable only after the client has written at least one byte: that is
/// what `ImpersonateNamedPipeClient` requires, and the server-side read of
/// the connection's first byte is where a transport calls this. The SID is
/// the identity; the pid is an observation and MUST NOT be authorized on
/// [0010 §4.4].
pub fn client_principal(server: &NamedPipeServer) -> Result<WindowsPrincipal, Error> {
    weida_winpipe::client_peer(server)
        .map(into_principal)
        .map_err(Error::Io)
}

/// The principal the kernel attributes to the process that created the pipe
/// `client` is connected to: the pipe object's owner.
pub fn server_principal(client: &NamedPipeClient) -> Result<WindowsPrincipal, Error> {
    weida_winpipe::server_peer(client)
        .map(into_principal)
        .map_err(Error::Io)
}

fn into_principal(peer: weida_winpipe::PipePeer) -> WindowsPrincipal {
    WindowsPrincipal {
        sid: peer.sid.into(),
        pid: Some(peer.pid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn name(tag: &str) -> String {
        format!(r"\\.\pipe\weida-runtime-{}-{tag}", std::process::id())
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: both ends learn the same account, which is this process's,
        /// and the server learns it only after the client wrote.
        #[tokio::test]
        async fn both_ends_name_this_process() {
            let exec = Exec::current().expect("runtime");
            let path = name("who");
            let (bound, server) = BoundPipe::bind(path.clone()).expect("bind");
            let client = connect_pipe(&exec, bound.path()).await.expect("connect");
            server.connect().await.expect("accept");

            let mut client = client;
            let server_seen_by_client = server_principal(&client).expect("owner");
            client.write_all(b"x").await.expect("write");
            let mut byte = [0u8; 1];
            let mut server = server;
            server.read_exact(&mut byte).await.expect("read");
            let client_seen_by_server = client_principal(&server).expect("token");

            assert_eq!(client_seen_by_server.sid, server_seen_by_client.sid);
            assert_eq!(client_seen_by_server.pid, Some(std::process::id()));
            assert_eq!(server_seen_by_client.pid, Some(std::process::id()));
        }
        /// Claim: a name that exists is refused, not shared.
        #[tokio::test]
        async fn a_taken_name_is_not_bound_twice() {
            let path = name("taken");
            let (_bound, _first) = BoundPipe::bind(path.clone()).expect("bind");
            let err = BoundPipe::bind(path).unwrap_err();
            assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
        }
        /// Claim: a pipe nobody serves is the peer being gone.
        #[tokio::test]
        async fn an_absent_pipe_is_a_closed_peer() {
            let exec = Exec::current().expect("runtime");
            let path = name("absent");
            let err = connect_pipe(&exec, std::ffi::OsStr::new(&path))
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::ConnectionLost(LossCause::PeerClosed)),
                "{err:?}"
            );
        }
        /// Claim: a pipe that stays busy is waited out, however long, and the
        /// open completes once the server makes an instance: busy is
        /// backpressure, not a failure. The minute of busy is on a paused
        /// clock; the pipe itself is real.
        #[tokio::test(start_paused = true)]
        async fn a_pipe_busy_for_a_minute_is_still_opened() {
            let exec = Exec::current().expect("runtime");
            let path = name("busy");
            let (bound, first) = BoundPipe::bind(path.clone()).expect("bind");
            // The only instance, taken: the next open finds the pipe busy.
            let _taken = connect_pipe(&exec, bound.path()).await.expect("connect");
            first.connect().await.expect("accept");

            let waiting = {
                let exec = exec.clone();
                let path = bound.path().to_owned();
                tokio::spawn(async move { connect_pipe(&exec, &path).await })
            };
            tokio::time::sleep(Duration::from_secs(60)).await;
            assert!(!waiting.is_finished(), "the open gave up on a busy pipe");

            let next = bound.next_instance().expect("next instance");
            let opened = tokio::time::timeout(Duration::from_secs(5), waiting)
                .await
                .expect("the open completes once an instance exists")
                .expect("task");
            assert!(opened.is_ok(), "{opened:?}");
            next.connect().await.expect("accept");
        }
    }
}
