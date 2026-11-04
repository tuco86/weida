//! A resolver whose lookups the caller reads from a queue and answers
//! (docs/c/resolver.md).

use std::ffi::{c_char, c_int};
use std::net::SocketAddr;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

use ::weida_runtime::Exec;
use tokio::sync::{mpsc, oneshot};
use weida_rs::{Error, Resolved, Resolver, SharedResolver};

use crate::error::{
    WEIDA_ECLOSED, WEIDA_EINVAL, closed, fail_abi, guard, guard_or, guard_ptr, guard_void, null_arg,
};
use crate::handle::{
    CText, borrow, cstr, into_handle, names, out_ptr, release, text_out, write_out,
};
use crate::try_handle::Tried;

/// The answer a lookup waits for: the addresses, or why there are none.
type Answer = Result<Vec<SocketAddr>, String>;

/// One `Resolver::resolve` call, queued for the caller.
struct Lookup {
    name: String,
    port: Option<u16>,
    max_addresses: usize,
    answer: oneshot::Sender<Answer>,
}

/// The `Resolver` a configuration holds: it queues each lookup and awaits
/// its answer.
#[derive(Debug)]
struct QueueResolver {
    lookups: mpsc::Sender<Lookup>,
}

impl Resolver for QueueResolver {
    fn resolve<'a>(
        &'a self,
        _exec: &'a Exec,
        name: &'a str,
        port: Option<u16>,
        max_addresses: usize,
    ) -> Resolved<'a> {
        // A dial that stops waiting (a deadline) drops this future; an
        // answer that comes later is discarded.
        Box::pin(async move {
            let (answer, answered) = oneshot::channel();
            let lookup = Lookup {
                name: name.to_owned(),
                port,
                max_addresses,
                answer,
            };
            if self.lookups.send(lookup).await.is_err() {
                return Err(Error::InvalidAddress(format!(
                    "cannot resolve {name}: the resolver was dropped"
                )));
            }
            match answered.await {
                Ok(Ok(addrs)) => Ok(addrs),
                Ok(Err(reason)) => Err(Error::InvalidAddress(format!(
                    "cannot resolve {name}: {reason}"
                ))),
                Err(_) => Err(Error::InvalidAddress(format!(
                    "cannot resolve {name}: the lookup was dropped unanswered"
                ))),
            }
        })
    }
}

/// A resolver whose lookups are read from a queue (`Resolver`, decision
/// 10), a try handle (R3a).
pub struct weida_resolver {
    tried: Tried<mpsc::Receiver<Lookup>>,
    queue: mpsc::WeakSender<Lookup>,
    /// The strong sender, held only until the resolver is first set on a
    /// configuration: from then on the configurations and runtimes hold the
    /// queue, and `weida_resolver_recv` ends once the last of them is gone.
    unclaimed: Mutex<Option<mpsc::Sender<Lookup>>>,
}

impl weida_resolver {
    /// The resolver a configuration holds: it shares this queue.
    pub(crate) fn shared(&self) -> Result<SharedResolver, c_int> {
        let unclaimed = self
            .unclaimed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let lookups = unclaimed.or_else(|| self.queue.upgrade()).ok_or_else(|| {
            fail_abi(
                WEIDA_ECLOSED,
                c"the resolver's queue ended: no configuration or runtime holds it",
            )
        })?;
        Ok(SharedResolver::new(QueueResolver { lookups }))
    }
}

/// One lookup waiting for its answer (`Resolved`).
pub struct weida_resolved {
    name: CText,
    port: Option<u16>,
    max_addresses: usize,
    answer: Mutex<Option<oneshot::Sender<Answer>>>,
}

impl weida_resolved {
    fn lock(&self) -> MutexGuard<'_, Option<oneshot::Sender<Answer>>> {
        self.answer.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The addresses of an answer, or the code for the caller and the
    /// reason for the dial.
    ///
    /// # Safety
    ///
    /// As `weida_resolved_answer`'s `addrs` and `n`.
    unsafe fn parse(
        &self,
        addrs: *const *const c_char,
        n: usize,
    ) -> Result<Vec<SocketAddr>, (c_int, String)> {
        if n == 0 {
            return Err((
                fail_abi(WEIDA_EINVAL, c"an answer holds at least one address"),
                "the resolver answered no address".to_owned(),
            ));
        }
        if n > self.max_addresses {
            return Err((
                fail_abi(
                    WEIDA_EINVAL,
                    c"the answer holds more addresses than the lookup's max_addresses",
                ),
                format!(
                    "the resolver answered {n} addresses, more than the {} asked for",
                    self.max_addresses
                ),
            ));
        }
        // SAFETY: forwarded from this function's contract.
        let texts = unsafe { names(addrs, n) }.map_err(|code| {
            (
                code,
                "the resolver's answer held a NULL or non-UTF-8 address".to_owned(),
            )
        })?;
        texts
            .iter()
            .map(|text| {
                text.parse::<SocketAddr>().map_err(|_| {
                    (
                        fail_abi(WEIDA_EINVAL, c"an address is not an IP address and port"),
                        format!(
                            "the resolver answered `{text}`, which is not an IP address and port"
                        ),
                    )
                })
            })
            .collect()
    }
}

/// Makes a resolver whose lookups are read from a queue: set it on a
/// configuration with `weida_runtime_config_set_resolver`, then answer
/// each lookup `weida_resolver_recv` takes.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_resolver_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolver_new(out: *mut *mut weida_resolver) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // Lookups a resolver holds unread; a dial waits for a place beyond
        // them.
        const LOOKUP_QUEUE: usize = 64;
        let (tx, rx) = mpsc::channel(LOOKUP_QUEUE);
        let handle = into_handle(weida_resolver {
            tried: Tried::new(rx),
            queue: tx.downgrade(),
            unclaimed: Mutex::new(Some(tx)),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The resolver's readiness descriptor (R11): readable when the next
/// `weida_resolver_recv` will not block.
///
/// # Safety
///
/// `r` is NULL or a live resolver; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolver_fd(r: *const weida_resolver, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let fd = r.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next lookup, waiting up to `timeout_ms` (R4) for one. Every
/// dial and redial of a runtime made with this resolver asks it, IP
/// literals included.
///
/// Writes NULL to `out` and returns 0 once no configuration and no runtime
/// holds the resolver; a resolver never set on a configuration never ends.
///
/// # Safety
///
/// `r` is NULL or a live resolver; `out` is NULL or writable. Answer the
/// lookup with `weida_resolved_answer` or `_refuse`, or release it with
/// `weida_resolved_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolver_recv(
    r: *const weida_resolver,
    timeout_ms: i64,
    out: *mut *mut weida_resolved,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        // No future is kept between tries: `poll_recv` loses nothing.
        let lookup = r.tried.attempt(timeout_ms, |rx, cx| rx.poll_recv(cx))?;
        let handle = lookup.map_or(ptr::null_mut(), |lookup| {
            into_handle(weida_resolved {
                name: CText::new(lookup.name),
                port: lookup.port,
                max_addresses: lookup.max_addresses,
                answer: Mutex::new(Some(lookup.answer)),
            })
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases the resolver's queue. A thread waiting in
/// `weida_resolver_recv` returns `-WEIDA_ECLOSED`; the lookups still
/// queued and every later one fail their dial with
/// `-WEIDA_EINVALID_ADDRESS`. NULL does nothing.
///
/// # Safety
///
/// `r` is NULL or a live resolver, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolver_drop(r: *mut weida_resolver) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(r) = unsafe { release(r) } {
            drop(r.tried.close());
        }
    });
}

/// The host the URL wrote; NULL for a NULL `q`. Lives until `q` is
/// consumed or dropped.
///
/// # Safety
///
/// `q` is NULL or a live lookup; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolved_name(
    q: *const weida_resolved,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if q.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `q` is a live lookup by this function's
        // contract; the text lives as long as it.
        let q = unsafe { &*q };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&q.name), len_out) }
    })
}

/// 1 and the port when the URL wrote one; 0 when it wrote none (the
/// authority names a set, and the answer chooses the ports), also for a
/// NULL `q` or `out`.
///
/// # Safety
///
/// `q` is NULL or a live lookup; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolved_port(q: *const weida_resolved, out: *mut u16) -> c_int {
    guard_or(0, || {
        if q.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `q` is a live lookup by the caller's contract.
        let q = unsafe { &*q };
        match q.port {
            Some(port) => {
                // SAFETY: a non-NULL `out` is writable by the caller's contract.
                unsafe { out.write(port) };
                1
            }
            None => 0,
        }
    })
}

/// The most addresses the answer may hold (`RuntimeConfig::max_resolved_addresses`);
/// 0 for a NULL `q`.
///
/// # Safety
///
/// `q` is NULL or a live lookup.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolved_max_addresses(q: *const weida_resolved) -> u64 {
    guard_or(0, || {
        if q.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `q` is a live lookup by the caller's contract.
        let q = unsafe { &*q };
        u64::try_from(q.max_addresses).unwrap_or(u64::MAX)
    })
}

/// Answers the lookup with `n` addresses, `ip:port` texts dialled in this
/// order (`127.0.0.1:4433`, `[::1]:4433`). **Consumes** `q` on every
/// return path.
///
/// `-WEIDA_EINVAL` for no address, more than
/// `weida_resolved_max_addresses`, a NULL or non-UTF-8 element, or a text
/// that is not an IP address and port; the lookup is then refused, and its
/// dial fails with `-WEIDA_EINVALID_ADDRESS` naming why.
///
/// # Safety
///
/// `q` is NULL or a live lookup, not used again; `addrs` is NULL or points
/// to `n` readable pointers, each NULL or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolved_answer(
    q: *mut weida_resolved,
    addrs: *const *const c_char,
    n: usize,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let q = unsafe { release(q) }.ok_or_else(null_arg)?;
        let answer = q.lock().take().ok_or_else(closed)?;
        // SAFETY: forwarded from this function's contract.
        match unsafe { q.parse(addrs, n) } {
            Ok(addrs) => {
                // The dial may be gone (a deadline): nobody to tell.
                let _ = answer.send(Ok(addrs));
                Ok(0)
            }
            Err((code, reason)) => {
                let _ = answer.send(Err(reason));
                Err(code)
            }
        }
    })
}

/// Refuses the lookup: its dial fails with `-WEIDA_EINVALID_ADDRESS`, the
/// message `cannot resolve <name>: <reason>`. **Consumes** `q` on every
/// return path; a NULL or non-UTF-8 `reason` still refuses, with a reason
/// of its own, and returns `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `q` is NULL or a live lookup, not used again; `reason` is NULL or a
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolved_refuse(
    q: *mut weida_resolved,
    reason: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let q = unsafe { release(q) }.ok_or_else(null_arg)?;
        let answer = q.lock().take().ok_or_else(closed)?;
        // SAFETY: forwarded from this function's contract.
        match unsafe { cstr(reason) } {
            Ok(reason) => {
                let _ = answer.send(Err(reason.to_owned()));
                Ok(0)
            }
            Err(code) => {
                let _ = answer.send(Err("the resolver refused the lookup".to_owned()));
                Err(code)
            }
        }
    })
}

/// Releases a lookup; one not answered fails its dial with
/// `-WEIDA_EINVALID_ADDRESS` ("the lookup was dropped unanswered"). NULL
/// does nothing.
///
/// # Safety
///
/// `q` is NULL or a live lookup, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_resolved_drop(q: *mut weida_resolved) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(q) }));
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::error::WEIDA_EAGAIN;
    use crate::runtime::{
        weida_runtime_config, weida_runtime_config_drop, weida_runtime_config_new,
        weida_runtime_config_set_resolver,
    };

    type Dial = tokio::task::JoinHandle<Result<Vec<SocketAddr>, Error>>;

    fn resolver() -> *mut weida_resolver {
        let mut r = ptr::null_mut();
        // SAFETY: `r` is writable.
        assert_eq!(unsafe { weida_resolver_new(&mut r) }, 0);
        r
    }

    /// A configuration holding `r`.
    fn config_with(r: *const weida_resolver) -> *mut weida_runtime_config {
        let mut cfg = ptr::null_mut();
        // SAFETY: `cfg` is writable, then live; `r` is live.
        unsafe {
            assert_eq!(weida_runtime_config_new(&mut cfg), 0);
            assert_eq!(weida_runtime_config_set_resolver(cfg, r), 0);
        }
        cfg
    }

    /// One dial's resolution through the resolver `cfg` holds now.
    fn dial(
        cfg: *mut weida_runtime_config,
        name: &'static str,
        port: Option<u16>,
        max: usize,
    ) -> Dial {
        let exec = Exec::current().expect("ambient runtime");
        // SAFETY: `cfg` is live.
        let resolver = unsafe { &*cfg }.config.lock().unwrap().resolver.clone();
        tokio::spawn(async move { resolver.resolve(&exec, name, port, max).await })
    }

    /// The next lookup, which must be queued already.
    fn recv(r: *const weida_resolver) -> *mut weida_resolved {
        let mut q = ptr::null_mut();
        // SAFETY: a live resolver and a writable out-parameter.
        assert_eq!(unsafe { weida_resolver_recv(r, 0, &mut q) }, 0);
        assert!(!q.is_null(), "the queue ended");
        q
    }

    /// The `InvalidAddress` message a dial failed with.
    async fn failure(dial: Dial) -> String {
        match dial.await.expect("the dial task") {
            Err(Error::InvalidAddress(m)) => m,
            other => panic!("expected InvalidAddress, got {other:?}"),
        }
    }

    /// Dials through `cfg`, answers the lookup with `addrs`, and returns
    /// the answer's code and the dial's failure.
    async fn answered(
        r: *const weida_resolver,
        cfg: *mut weida_runtime_config,
        addrs: &[*const c_char],
    ) -> (c_int, String) {
        let dialled = dial(cfg, "svc.weida.test", Some(7443), 2);
        tokio::task::yield_now().await;
        // SAFETY: a live lookup and `addrs.len()` readable pointers.
        let rc = unsafe { weida_resolved_answer(recv(r), addrs.as_ptr(), addrs.len()) };
        (rc, failure(dialled).await)
    }

    #[tokio::test(start_paused = true)]
    async fn a_lookup_reads_back_and_its_answer_reaches_the_dial() {
        let r = resolver();
        let cfg = config_with(r);
        let first = dial(cfg, "svc.weida.test", Some(7443), 8);
        let mut q = ptr::null_mut();
        // SAFETY: a live resolver and a writable out-parameter.
        assert_eq!(unsafe { weida_resolver_recv(r, 0, &mut q) }, -WEIDA_EAGAIN);
        tokio::task::yield_now().await;

        let q = recv(r);
        let mut len = 0usize;
        let mut port = 0u16;
        // SAFETY: a live lookup; the name lives as long as it.
        unsafe {
            let name = weida_resolved_name(q, &mut len);
            assert_eq!(CStr::from_ptr(name).to_str(), Ok("svc.weida.test"));
            assert_eq!(len, 14);
            assert_eq!(weida_resolved_port(q, &mut port), 1);
            assert_eq!(port, 7443);
            assert_eq!(weida_resolved_max_addresses(q), 8);
        }
        let addrs = [c"127.0.0.1:7443".as_ptr(), c"[::1]:7444".as_ptr()];
        // SAFETY: a live lookup and two readable pointers.
        assert_eq!(unsafe { weida_resolved_answer(q, addrs.as_ptr(), 2) }, 0);
        let expected: Vec<SocketAddr> = vec![
            "127.0.0.1:7443".parse().unwrap(),
            "[::1]:7444".parse().unwrap(),
        ];
        assert_eq!(first.await.expect("the dial task").ok(), Some(expected));

        let second = dial(cfg, "set.weida.test", None, 8);
        tokio::task::yield_now().await;
        let q = recv(r);
        let mut port = 1u16;
        // SAFETY: a live lookup, then released once.
        unsafe {
            assert_eq!(weida_resolved_port(q, &mut port), 0);
            assert_eq!(port, 1, "a portless lookup leaves `out` unwritten");
            weida_resolved_drop(q);
        }
        failure(second).await;
        // SAFETY: both handles are live and released once.
        unsafe {
            weida_runtime_config_drop(cfg);
            weida_resolver_drop(r);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_outside_the_contract_fails_the_dial() {
        let r = resolver();
        let cfg = config_with(r);
        let three = [
            c"127.0.0.1:1".as_ptr(),
            c"127.0.0.1:2".as_ptr(),
            c"127.0.0.1:3".as_ptr(),
        ];
        let cases: [(&[*const c_char], &str); 4] = [
            (&[], "no address"),
            (&three, "3 addresses, more than the 2"),
            (&[c"localhost:1".as_ptr()], "`localhost:1`, which is not"),
            (&[ptr::null()], "NULL or non-UTF-8"),
        ];
        for (addrs, expected) in cases {
            let (rc, message) = answered(r, cfg, addrs).await;
            assert_eq!(rc, -WEIDA_EINVAL, "{expected}");
            assert!(message.contains(expected), "{message}");
        }
        // SAFETY: both handles are live and released once.
        unsafe {
            weida_runtime_config_drop(cfg);
            weida_resolver_drop(r);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_or_dropped_lookup_fails_the_dial_with_invalid_address() {
        let r = resolver();
        let cfg = config_with(r);

        let dialled = dial(cfg, "svc.weida.test", Some(7443), 8);
        tokio::task::yield_now().await;
        // SAFETY: a live lookup, consumed.
        assert_eq!(
            unsafe { weida_resolved_refuse(recv(r), c"no such service".as_ptr()) },
            0
        );
        let message = failure(dialled).await;
        assert!(
            message.contains("cannot resolve svc.weida.test: no such service"),
            "{message}"
        );

        let dialled = dial(cfg, "svc.weida.test", Some(7443), 8);
        tokio::task::yield_now().await;
        // SAFETY: a live lookup, consumed.
        assert_eq!(
            unsafe { weida_resolved_refuse(recv(r), ptr::null()) },
            -WEIDA_EINVAL
        );
        let message = failure(dialled).await;
        assert!(message.contains("refused the lookup"), "{message}");

        let dialled = dial(cfg, "svc.weida.test", Some(7443), 8);
        tokio::task::yield_now().await;
        // SAFETY: a live lookup, released once.
        unsafe { weida_resolved_drop(recv(r)) };
        let message = failure(dialled).await;
        assert!(message.contains("dropped unanswered"), "{message}");

        let mut port = 0u16;
        // SAFETY: NULL handles, which every call accepts.
        unsafe {
            assert_eq!(
                weida_resolved_answer(ptr::null_mut(), ptr::null(), 0),
                -WEIDA_EINVAL
            );
            assert_eq!(
                weida_resolved_refuse(ptr::null_mut(), ptr::null()),
                -WEIDA_EINVAL
            );
            assert!(weida_resolved_name(ptr::null(), ptr::null_mut()).is_null());
            assert_eq!(weida_resolved_port(ptr::null(), &mut port), 0);
            assert_eq!(weida_resolved_max_addresses(ptr::null()), 0);
        }
        // SAFETY: both handles are live and released once.
        unsafe {
            weida_runtime_config_drop(cfg);
            weida_resolver_drop(r);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_queue_ends_once_no_configuration_holds_the_resolver() {
        let r = resolver();
        let cfg = config_with(r);
        let mut q = ptr::null_mut();
        // SAFETY: live handles and a writable out-parameter.
        unsafe {
            assert_eq!(weida_resolver_recv(r, 0, &mut q), -WEIDA_EAGAIN);
            assert_eq!(weida_runtime_config_set_resolver(cfg, ptr::null()), 0);
            assert_eq!(weida_resolver_recv(r, 0, &mut q), 0);
        }
        assert!(q.is_null(), "the queue ended");
        let system = dial(cfg, "127.0.0.1", Some(9), 8);
        let expected: Vec<SocketAddr> = vec!["127.0.0.1:9".parse().unwrap()];
        assert_eq!(system.await.expect("the dial task").ok(), Some(expected));
        // SAFETY: live handles.
        assert_eq!(
            unsafe { weida_runtime_config_set_resolver(cfg, r) },
            -WEIDA_ECLOSED
        );

        let r2 = resolver();
        let cfg2 = config_with(r2);
        // SAFETY: `r2` is live and released once.
        unsafe { weida_resolver_drop(r2) };
        let message = failure(dial(cfg2, "svc.weida.test", Some(7443), 8)).await;
        assert!(message.contains("the resolver was dropped"), "{message}");
        // SAFETY: every handle is live and released once.
        unsafe {
            weida_runtime_config_drop(cfg);
            weida_runtime_config_drop(cfg2);
            weida_resolver_drop(r);
        }
    }
}
