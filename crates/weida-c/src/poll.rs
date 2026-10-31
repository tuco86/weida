//! `weida_poll`: waiting on several readiness descriptors at once
//! (docs/c/poll.md).

use std::ffi::c_int;

use crate::error::{WEIDA_EINVAL, fail_abi, guard};

/// The descriptor is readable: the next try or wait on its handle will not
/// block (R11).
pub const WEIDA_POLLIN: i16 = 1;

/// One descriptor `weida_poll` waits on.
#[repr(C)]
pub struct weida_poll_item {
    /// A descriptor from a `_fd` function; a negative one is skipped.
    pub fd: i64,
    /// What to wait for: `WEIDA_POLLIN`.
    pub events: i16,
    /// What happened, written by `weida_poll`: `WEIDA_POLLIN` or 0.
    pub revents: i16,
}

/// Waits until at least one of `n` descriptors in `items` is readable, or
/// `timeout_ms` (R4) passes.
///
/// Returns the number of items whose `revents` is now non-zero, 0 when the
/// timeout passed, `-WEIDA_EINTR` when a signal reached this thread, and
/// `-WEIDA_EINVAL` when `n > 0` and `items` is NULL. An item whose `events`
/// lacks `WEIDA_POLLIN`, or whose `fd` is negative, is never reported.
///
/// # Safety
///
/// `items` is NULL or points to `n` writable items, and every non-negative
/// `fd` among them came from a `_fd` function of a handle that stays alive
/// for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_poll(
    items: *mut weida_poll_item,
    n: usize,
    timeout_ms: i64,
) -> c_int {
    guard(|| {
        let items: &mut [weida_poll_item] = if n == 0 {
            &mut []
        } else if items.is_null() {
            return Err(fail_abi(
                WEIDA_EINVAL,
                c"weida_poll was given items == NULL with n > 0",
            ));
        } else {
            // SAFETY: by this function's contract `items` points to `n`
            // writable items.
            unsafe { std::slice::from_raw_parts_mut(items, n) }
        };
        for item in items.iter_mut() {
            item.revents = 0;
        }
        poll_items(items, timeout_ms)
    })
}

fn wanted(item: &weida_poll_item) -> bool {
    item.fd >= 0 && item.events & WEIDA_POLLIN != 0
}

#[cfg(unix)]
fn poll_items(items: &mut [weida_poll_item], timeout_ms: i64) -> Result<c_int, c_int> {
    use crate::error::{WEIDA_EINTR, fail};
    use nix::errno::Errno;
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use std::os::fd::BorrowedFd;

    let mut at = Vec::with_capacity(items.len());
    let mut fds = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        if !wanted(item) {
            continue;
        }
        let raw = i32::try_from(item.fd)
            .map_err(|_| fail_abi(WEIDA_EINVAL, c"a weida_poll descriptor is out of range"))?;
        // SAFETY: by `weida_poll`'s contract the descriptor belongs to a
        // handle that stays alive for the call.
        let fd = unsafe { BorrowedFd::borrow_raw(raw) };
        fds.push(PollFd::new(fd, PollFlags::POLLIN));
        at.push(i);
    }
    let timeout = if timeout_ms < 0 {
        PollTimeout::NONE
    } else {
        PollTimeout::try_from(timeout_ms.min(i64::from(i32::MAX))).unwrap_or(PollTimeout::MAX)
    };
    match poll(&mut fds, timeout) {
        Ok(_) => {}
        Err(Errno::EINTR) => return Err(fail_abi(WEIDA_EINTR, c"interrupted by a signal")),
        Err(e) => return Err(fail(weida_rs::Error::Io(e.into()))),
    }
    let mut ready = 0;
    for (fd, i) in fds.iter().zip(at) {
        if fd.any().unwrap_or(true) {
            items[i].revents = WEIDA_POLLIN;
            ready += 1;
        }
    }
    Ok(ready)
}

#[cfg(windows)]
fn poll_items(items: &mut [weida_poll_item], timeout_ms: i64) -> Result<c_int, c_int> {
    use crate::error::fail;
    use windows_sys::Win32::Networking::WinSock::{POLLRDNORM, WSAPOLLFD, WSAPoll};

    let mut at = Vec::with_capacity(items.len());
    let mut fds = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        if wanted(item) {
            fds.push(WSAPOLLFD {
                fd: item.fd as usize,
                events: POLLRDNORM,
                revents: 0,
            });
            at.push(i);
        }
    }
    let timeout = if timeout_ms < 0 {
        -1
    } else {
        i32::try_from(timeout_ms).unwrap_or(i32::MAX)
    };
    if fds.is_empty() {
        // `WSAPoll` refuses an empty set; nothing can become ready, so this
        // is a sleep for the timeout, as `poll` with no descriptors is.
        if timeout >= 0 {
            std::thread::sleep(std::time::Duration::from_millis(timeout as u64));
            return Ok(0);
        }
        return Err(fail_abi(
            WEIDA_EINVAL,
            c"weida_poll was asked to wait forever on no descriptor",
        ));
    }
    let count = u32::try_from(fds.len())
        .map_err(|_| fail_abi(WEIDA_EINVAL, c"too many weida_poll items"))?;
    // SAFETY: `fds` is a live array of `count` elements for the call, and
    // each socket belongs to a handle that stays alive by `weida_poll`'s
    // contract.
    let n = unsafe { WSAPoll(fds.as_mut_ptr(), count, timeout) };
    if n < 0 {
        return Err(fail(weida_rs::Error::Io(std::io::Error::last_os_error())));
    }
    let mut ready = 0;
    for (fd, i) in fds.iter().zip(at) {
        if fd.revents != 0 {
            items[i].revents = WEIDA_POLLIN;
            ready += 1;
        }
    }
    Ok(ready)
}
