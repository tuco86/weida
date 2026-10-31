//! A handle's readiness descriptor (docs/c/README.md R11): an atomic flag
//! plus a socket pair, created lazily, whose read end is readable exactly
//! while the flag is raised.

use std::ffi::c_int;
use std::io::{ErrorKind, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Wake;

use crate::error::fail;
#[cfg(unix)]
type Stream = std::os::unix::net::UnixStream;
#[cfg(windows)]
type Stream = std::net::TcpStream;

/// Both ends of the descriptor: a byte written to `write` makes `read`
/// readable.
struct Pair {
    read: Stream,
    write: Stream,
}

impl Pair {
    #[cfg(unix)]
    fn new() -> std::io::Result<Pair> {
        let (read, write) = Stream::pair()?;
        read.set_nonblocking(true)?;
        write.set_nonblocking(true)?;
        Ok(Pair { read, write })
    }

    /// A loopback TCP pair: Windows has no socketpair, and `WSAPoll` takes
    /// sockets only. A stranger that connects to the listener first is
    /// dropped, so the read end is always the connector's peer.
    #[cfg(windows)]
    fn new() -> std::io::Result<Pair> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
        let write = Stream::connect(listener.local_addr()?)?;
        let ours = write.local_addr()?;
        let read = loop {
            let (stream, from) = listener.accept()?;
            if from == ours {
                break stream;
            }
        };
        read.set_nonblocking(true)?;
        write.set_nonblocking(true)?;
        write.set_nodelay(true)?;
        Ok(Pair { read, write })
    }

    fn descriptor(&self) -> i64 {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            i64::from(self.read.as_raw_fd())
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            self.read.as_raw_socket() as i64
        }
    }

    fn poke(&self) {
        // Full means a byte is already there, which is all a poke is for.
        let _ = (&self.write).write(&[1]);
    }

    fn drain(&self) {
        let mut sink = [0u8; 64];
        loop {
            match (&self.read).read(&mut sink) {
                Ok(0) => return,
                Ok(_) => continue,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return,
            }
        }
    }
}

/// The readiness of one handle: raised by a waker or a finished op, cleared
/// by the try that consumes it.
pub(crate) struct Signal {
    signalled: AtomicBool,
    pair: Mutex<Option<Pair>>,
}

impl Signal {
    pub(crate) fn new() -> Arc<Signal> {
        Arc::new(Signal {
            signalled: AtomicBool::new(false),
            pair: Mutex::new(None),
        })
    }

    fn pair(&self) -> MutexGuard<'_, Option<Pair>> {
        self.pair.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Marks the handle ready. Only the first raise after a clear writes a
    /// byte, and only once the descriptor exists.
    pub(crate) fn raise(&self) {
        if !self.signalled.swap(true, Ordering::AcqRel)
            && let Some(pair) = self.pair().as_ref()
        {
            pair.poke();
        }
    }

    /// Marks the handle not ready and empties the descriptor.
    ///
    /// The descriptor is drained whether or not the flag was set: a raise
    /// that swapped the flag just before this clear may write its byte just
    /// after it, and that stale byte must not keep the descriptor readable
    /// for good.
    pub(crate) fn clear(&self) {
        let pair = self.pair();
        self.signalled.store(false, Ordering::Release);
        if let Some(pair) = pair.as_ref() {
            pair.drain();
        }
    }

    #[cfg(test)]
    pub(crate) fn is_raised(&self) -> bool {
        self.signalled.load(Ordering::Acquire)
    }

    /// The descriptor, created on first use; readable at once when the
    /// handle is already ready.
    pub(crate) fn fd(&self) -> Result<i64, c_int> {
        let mut slot = self.pair();
        if let Some(pair) = slot.as_ref() {
            return Ok(pair.descriptor());
        }
        let pair = Pair::new().map_err(|e| fail(weida_rs::Error::Io(e)))?;
        if self.signalled.load(Ordering::Acquire) {
            pair.poke();
        }
        let descriptor = pair.descriptor();
        *slot = Some(pair);
        Ok(descriptor)
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.raise();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.raise();
    }
}

/// Waits until `fd` is readable: `Ok(true)` when it is, `Ok(false)` when
/// `timeout_ms` (negative: no bound) passed first.
#[cfg(unix)]
pub(crate) fn wait_readable(fd: i64, timeout_ms: i64) -> Result<bool, c_int> {
    use crate::error::{WEIDA_EINTR, fail_abi};
    use nix::errno::Errno;
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use std::os::fd::BorrowedFd;

    let raw = i32::try_from(fd).map_err(|_| crate::error::null_arg())?;
    // SAFETY: `fd` is the read end of a `Signal`'s pair, and the caller holds
    // that `Signal` (through its handle) for the whole call, so the
    // descriptor stays open while it is borrowed here.
    let borrowed = unsafe { BorrowedFd::borrow_raw(raw) };
    let mut fds = [PollFd::new(borrowed, PollFlags::POLLIN)];
    let timeout = if timeout_ms < 0 {
        PollTimeout::NONE
    } else {
        PollTimeout::try_from(timeout_ms.min(i64::from(i32::MAX))).unwrap_or(PollTimeout::MAX)
    };
    match poll(&mut fds, timeout) {
        Ok(0) => Ok(false),
        Ok(_) => Ok(true),
        Err(Errno::EINTR) => Err(fail_abi(WEIDA_EINTR, c"interrupted by a signal")),
        Err(e) => Err(fail(weida_rs::Error::Io(e.into()))),
    }
}

/// Waits until `fd` is readable: `Ok(true)` when it is, `Ok(false)` when
/// `timeout_ms` (negative: no bound) passed first.
#[cfg(windows)]
pub(crate) fn wait_readable(fd: i64, timeout_ms: i64) -> Result<bool, c_int> {
    use windows_sys::Win32::Networking::WinSock::{POLLRDNORM, WSAPOLLFD, WSAPoll};

    let mut fds = [WSAPOLLFD {
        fd: fd as usize,
        events: POLLRDNORM,
        revents: 0,
    }];
    let timeout = if timeout_ms < 0 {
        -1
    } else {
        i32::try_from(timeout_ms).unwrap_or(i32::MAX)
    };
    // SAFETY: `fds` is a live array of one element for the duration of the
    // call, and its socket is the read end of a `Signal` the caller holds.
    let n = unsafe { WSAPoll(fds.as_mut_ptr(), 1, timeout) };
    match n {
        0 => Ok(false),
        n if n > 0 => Ok(true),
        _ => Err(fail(weida_rs::Error::Io(std::io::Error::last_os_error()))),
    }
}
