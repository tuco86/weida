//! Status codes, the thread-local record of the last failure, and the panic
//! guards every export runs in (docs/c/README.md R9, R10).

use std::any::Any;
use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use weida_rs::{Error, LossCause};

/// `Error::Runtime`: the runtime could not be created or used.
pub const WEIDA_ERUNTIME: c_int = 1;
/// `Error::InvalidAddress`: a `weida://` URL or a bind address did not parse.
pub const WEIDA_EINVALID_ADDRESS: c_int = 2;
/// `Error::InvalidEndpointPath`: an endpoint path broke the addressing rules.
pub const WEIDA_EINVALID_ENDPOINT_PATH: c_int = 3;
/// `Error::InvalidFingerprint`: not `sha256:` plus 64 hex digits.
pub const WEIDA_EINVALID_FINGERPRINT: c_int = 4;
/// `Error::AlreadyRegistered`: the path is taken on this listener.
pub const WEIDA_EALREADY_REGISTERED: c_int = 5;
/// `Error::NotConnected`: no usable peer connection.
pub const WEIDA_ENOT_CONNECTED: c_int = 6;
/// `Error::ConnectionLost`: definitely not delivered; see
/// `weida_last_error_loss_cause`.
pub const WEIDA_ECONNECTION_LOST: c_int = 7;
/// `Error::Negotiation`: version or capability negotiation failed.
pub const WEIDA_ENEGOTIATION: c_int = 8;
/// `Error::Protocol`: the peer violated the wire protocol.
pub const WEIDA_EPROTOCOL: c_int = 9;
/// `Error::Rejected`: the peer refused the transfer.
pub const WEIDA_EREJECTED: c_int = 10;
/// `Error::UnknownEndpoint`: the peer has no endpoint at that path.
pub const WEIDA_EUNKNOWN_ENDPOINT: c_int = 11;
/// `Error::Unsupported`: the peer does not support a requested feature.
pub const WEIDA_EUNSUPPORTED: c_int = 12;
/// `Error::NoParkedConnection`: no parked local connection for a stream.
pub const WEIDA_ENO_PARKED_CONNECTION: c_int = 13;
/// `Error::NoReply`: the request was accepted and never answered.
pub const WEIDA_ENO_REPLY: c_int = 14;
/// `Error::Canceled`: the transfer was canceled, locally or by the peer.
pub const WEIDA_ECANCELED: c_int = 15;
/// `Error::Expired`: the transfer's deadline passed.
pub const WEIDA_EEXPIRED: c_int = 16;
/// `Error::Indeterminate`: the outcome is unknown.
pub const WEIDA_EINDETERMINATE: c_int = 17;
/// `Error::LimitExceeded`: a resource limit or a ceiling was reached.
pub const WEIDA_ELIMIT_EXCEEDED: c_int = 18;
/// `Error::DatagramsUnavailable`: the connection carries no datagrams.
pub const WEIDA_EDATAGRAMS_UNAVAILABLE: c_int = 19;
/// `Error::TooLarge`: see `weida_last_error_max`.
pub const WEIDA_ETOO_LARGE: c_int = 20;
/// `Error::Tls`: TLS material or the handshake failed.
pub const WEIDA_ETLS: c_int = 21;
/// `Error::Untrusted`: see `weida_last_error_fingerprint`.
pub const WEIDA_EUNTRUSTED: c_int = 22;
/// `Error::Io`: see `weida_last_error_os_code`.
pub const WEIDA_EIO: c_int = 23;
/// `Error::Transport`: a transport failure outside the modelled outcomes.
pub const WEIDA_ETRANSPORT: c_int = 24;

/// The wait gave up: nothing was ready within the timeout (R4).
pub const WEIDA_EAGAIN: c_int = 100;
/// A signal reached the waiting thread (R4). Never on Windows.
pub const WEIDA_EINTR: c_int = 101;
/// A required pointer was NULL, a string was not UTF-8, or an argument was
/// out of range.
pub const WEIDA_EINVAL: c_int = 102;
/// The handle was dropped, spent or consumed.
pub const WEIDA_ECLOSED: c_int = 103;
/// A panic was caught at the boundary (R10).
pub const WEIDA_EPANIC: c_int = 104;

/// `LossCause::IdleTimeout`.
pub const WEIDA_LOSS_IDLE_TIMEOUT: c_int = 1;
/// `LossCause::PeerClosed`.
pub const WEIDA_LOSS_PEER_CLOSED: c_int = 2;
/// `LossCause::PeerRestarting`.
pub const WEIDA_LOSS_PEER_RESTARTING: c_int = 3;
/// `LossCause::LocallyClosed`.
pub const WEIDA_LOSS_LOCALLY_CLOSED: c_int = 4;
/// `LossCause::Reset`.
pub const WEIDA_LOSS_RESET: c_int = 5;
/// `LossCause::TransportError`.
pub const WEIDA_LOSS_TRANSPORT_ERROR: c_int = 6;

/// The code of every variant. One exhaustive match, so a new variant does
/// not compile without one.
pub(crate) fn code(e: &Error) -> c_int {
    match e {
        Error::Runtime(_) => WEIDA_ERUNTIME,
        Error::InvalidAddress(_) => WEIDA_EINVALID_ADDRESS,
        Error::InvalidEndpointPath => WEIDA_EINVALID_ENDPOINT_PATH,
        Error::InvalidFingerprint(_) => WEIDA_EINVALID_FINGERPRINT,
        Error::AlreadyRegistered => WEIDA_EALREADY_REGISTERED,
        Error::NotConnected => WEIDA_ENOT_CONNECTED,
        Error::ConnectionLost(_) => WEIDA_ECONNECTION_LOST,
        Error::Negotiation(_) => WEIDA_ENEGOTIATION,
        Error::Protocol(_) => WEIDA_EPROTOCOL,
        Error::Rejected => WEIDA_EREJECTED,
        Error::UnknownEndpoint => WEIDA_EUNKNOWN_ENDPOINT,
        Error::Unsupported => WEIDA_EUNSUPPORTED,
        Error::NoParkedConnection => WEIDA_ENO_PARKED_CONNECTION,
        Error::NoReply => WEIDA_ENO_REPLY,
        Error::Canceled => WEIDA_ECANCELED,
        Error::Expired => WEIDA_EEXPIRED,
        Error::Indeterminate => WEIDA_EINDETERMINATE,
        Error::LimitExceeded => WEIDA_ELIMIT_EXCEEDED,
        Error::DatagramsUnavailable => WEIDA_EDATAGRAMS_UNAVAILABLE,
        Error::TooLarge { .. } => WEIDA_ETOO_LARGE,
        Error::Tls(_) => WEIDA_ETLS,
        Error::Untrusted(_) => WEIDA_EUNTRUSTED,
        Error::Io(_) => WEIDA_EIO,
        Error::Transport(_) => WEIDA_ETRANSPORT,
    }
}

fn loss_code(cause: LossCause) -> c_int {
    match cause {
        LossCause::IdleTimeout => WEIDA_LOSS_IDLE_TIMEOUT,
        LossCause::PeerClosed => WEIDA_LOSS_PEER_CLOSED,
        LossCause::PeerRestarting => WEIDA_LOSS_PEER_RESTARTING,
        LossCause::LocallyClosed => WEIDA_LOSS_LOCALLY_CLOSED,
        LossCause::Reset => WEIDA_LOSS_RESET,
        LossCause::TransportError => WEIDA_LOSS_TRANSPORT_ERROR,
    }
}

/// The text of a record: a constant for the ABI's own failures, so an
/// `EAGAIN` allocates nothing.
enum Message {
    Static(&'static CStr),
    Owned(CString),
}

/// What the last failing call on this thread said.
struct Record {
    message: Message,
    loss_cause: c_int,
    fingerprint: Option<CString>,
    max: i64,
    os_code: c_int,
}

thread_local! {
    static LAST: RefCell<Option<Record>> = const { RefCell::new(None) };
}

fn store(record: Record) {
    LAST.with_borrow_mut(|last| *last = Some(record));
}

/// Text with interior NULs replaced by U+FFFD, so it fits a C string whole.
fn c_text(text: &str) -> CString {
    CString::new(text.replace('\0', "\u{FFFD}")).expect("no NUL is left")
}

/// Records `e` on this thread and returns its negative code.
pub(crate) fn fail(e: Error) -> c_int {
    let code = code(&e);
    let (loss_cause, fingerprint, max, os_code) = match &e {
        Error::ConnectionLost(cause) => (loss_code(*cause), None, -1, 0),
        Error::Untrusted(fp) => (0, Some(c_text(&fp.to_string())), -1, 0),
        Error::TooLarge { max } => (0, None, i64::try_from(*max).unwrap_or(i64::MAX), 0),
        Error::Io(io) => (0, None, -1, io.raw_os_error().unwrap_or(0)),
        _ => (0, None, -1, 0),
    };
    store(Record {
        message: Message::Owned(c_text(&e.to_string())),
        loss_cause,
        fingerprint,
        max,
        os_code,
    });
    -code
}

/// Records one of the ABI's own failures and returns its negative code.
pub(crate) fn fail_abi(code: c_int, message: &'static CStr) -> c_int {
    store(Record {
        message: Message::Static(message),
        loss_cause: 0,
        fingerprint: None,
        max: -1,
        os_code: 0,
    });
    -code
}

/// `-WEIDA_EINVAL` for a NULL where a pointer is required.
pub(crate) fn null_arg() -> c_int {
    fail_abi(WEIDA_EINVAL, c"a required pointer argument was NULL")
}

/// `-WEIDA_ECLOSED` for a handle that was dropped, spent or consumed.
pub(crate) fn closed() -> c_int {
    fail_abi(WEIDA_ECLOSED, c"the handle was dropped, spent or consumed")
}

fn panic_text(payload: &(dyn Any + Send)) -> String {
    let what = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a non-text payload");
    format!("panic: {what}")
}

/// Runs an `int` export: `Ok(n)` returns `n`, `Err(code)` returns the
/// negative code its failure recorded, and a panic is recorded and returned
/// as `-WEIDA_EPANIC` (R10).
pub(crate) fn guard(f: impl FnOnce() -> Result<c_int, c_int>) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(n)) => n,
        Ok(Err(code)) => code,
        Err(payload) => {
            store(Record {
                message: Message::Owned(c_text(&panic_text(payload.as_ref()))),
                loss_cause: 0,
                fingerprint: None,
                max: -1,
                os_code: 0,
            });
            -WEIDA_EPANIC
        }
    }
}

/// Runs a pointer getter: a panic returns NULL (R10).
pub(crate) fn guard_ptr<T>(f: impl FnOnce() -> *const T) -> *const T {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(ptr::null())
}

/// Runs an infallible scalar accessor: a panic returns `fallback`.
pub(crate) fn guard_or<T>(fallback: T, f: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(fallback)
}

/// Runs a `_drop`: a panic is swallowed (R10).
pub(crate) fn guard_void(f: impl FnOnce()) {
    let _ = catch_unwind(AssertUnwindSafe(f));
}

fn description(code: c_int) -> &'static CStr {
    match code {
        WEIDA_ERUNTIME => c"runtime error",
        WEIDA_EINVALID_ADDRESS => c"invalid address",
        WEIDA_EINVALID_ENDPOINT_PATH => c"invalid endpoint path",
        WEIDA_EINVALID_FINGERPRINT => c"invalid fingerprint",
        WEIDA_EALREADY_REGISTERED => c"endpoint path already registered",
        WEIDA_ENOT_CONNECTED => c"endpoint is not connected to any peer",
        WEIDA_ECONNECTION_LOST => c"connection lost before the transfer completed",
        WEIDA_ENEGOTIATION => c"negotiation failed",
        WEIDA_EPROTOCOL => c"protocol violation",
        WEIDA_EREJECTED => c"peer rejected the transfer",
        WEIDA_EUNKNOWN_ENDPOINT => c"peer has no such endpoint",
        WEIDA_EUNSUPPORTED => c"peer does not support the requested feature",
        WEIDA_ENO_PARKED_CONNECTION => c"peer has no parked connection for a stream toward it",
        WEIDA_ENO_REPLY => c"peer accepted the request but sent no reply",
        WEIDA_ECANCELED => c"transfer canceled",
        WEIDA_EEXPIRED => c"the transfer expired before the peer acknowledged it",
        WEIDA_EINDETERMINATE => c"outcome indeterminate",
        WEIDA_ELIMIT_EXCEEDED => c"resource limit exceeded",
        WEIDA_EDATAGRAMS_UNAVAILABLE => c"datagrams are not available on this connection",
        WEIDA_ETOO_LARGE => c"payload exceeds the largest datagram this connection carries",
        WEIDA_ETLS => c"tls error",
        WEIDA_EUNTRUSTED => c"peer identity is not trusted",
        WEIDA_EIO => c"io error",
        WEIDA_ETRANSPORT => c"transport error",
        WEIDA_EAGAIN => c"nothing was ready within the timeout",
        WEIDA_EINTR => c"interrupted by a signal",
        WEIDA_EINVAL => c"invalid argument",
        WEIDA_ECLOSED => c"the handle was dropped, spent or consumed",
        WEIDA_EPANIC => c"a panic was caught at the boundary",
        _ => c"unknown error",
    }
}

/// The description of a status code, positive or negative.
///
/// Returns `"unknown error"` for a code this library does not define. The
/// string is static.
///
/// # Safety
///
/// Always safe to call; it is `unsafe` like every export of this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_strerror(code: c_int) -> *const c_char {
    guard_ptr(|| description(code.checked_abs().unwrap_or(0)).as_ptr())
}

/// The `Display` text of the last failure on this thread, or NULL when no
/// call on this thread has failed. Interior NULs are replaced by U+FFFD.
///
/// Valid until the next failing call on this thread.
///
/// # Safety
///
/// Always safe to call; the pointer must not be used after the next failing
/// call on the same thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_last_error_message() -> *const c_char {
    guard_ptr(|| {
        LAST.with_borrow(|last| match last {
            Some(Record {
                message: Message::Static(text),
                ..
            }) => text.as_ptr(),
            Some(Record {
                message: Message::Owned(text),
                ..
            }) => text.as_ptr(),
            None => ptr::null(),
        })
    })
}

/// The `WEIDA_LOSS_*` cause of the last failure on this thread when it was
/// `WEIDA_ECONNECTION_LOST`, otherwise 0.
///
/// # Safety
///
/// Always safe to call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_last_error_loss_cause() -> c_int {
    guard_or(0, || {
        LAST.with_borrow(|last| last.as_ref().map_or(0, |r| r.loss_cause))
    })
}

/// The fingerprint (`sha256:` text) the peer presented when the last failure
/// on this thread was `WEIDA_EUNTRUSTED`, otherwise NULL.
///
/// Valid until the next failing call on this thread.
///
/// # Safety
///
/// Always safe to call; the pointer must not be used after the next failing
/// call on the same thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_last_error_fingerprint() -> *const c_char {
    guard_ptr(|| {
        LAST.with_borrow(|last| {
            last.as_ref()
                .and_then(|r| r.fingerprint.as_ref())
                .map_or(ptr::null(), |fp| fp.as_ptr())
        })
    })
}

/// The largest payload the connection carries when the last failure on this
/// thread was `WEIDA_ETOO_LARGE`, otherwise -1.
///
/// # Safety
///
/// Always safe to call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_last_error_max() -> i64 {
    guard_or(-1, || {
        LAST.with_borrow(|last| last.as_ref().map_or(-1, |r| r.max))
    })
}

/// The raw OS error when the last failure on this thread was `WEIDA_EIO`
/// and carried one, otherwise 0.
///
/// # Safety
///
/// Always safe to call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_last_error_os_code() -> c_int {
    guard_or(0, || {
        LAST.with_borrow(|last| last.as_ref().map_or(0, |r| r.os_code))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use weida_rs::Fingerprint;

    #[test]
    fn every_variant_has_its_own_code_below_the_abi_codes() {
        let variants = [
            Error::Runtime("x".into()),
            Error::InvalidAddress("x".into()),
            Error::InvalidEndpointPath,
            Error::InvalidFingerprint("x".into()),
            Error::AlreadyRegistered,
            Error::NotConnected,
            Error::ConnectionLost(LossCause::IdleTimeout),
            Error::Negotiation("x".into()),
            Error::Protocol("x".into()),
            Error::Rejected,
            Error::UnknownEndpoint,
            Error::Unsupported,
            Error::NoParkedConnection,
            Error::NoReply,
            Error::Canceled,
            Error::Expired,
            Error::Indeterminate,
            Error::LimitExceeded,
            Error::DatagramsUnavailable,
            Error::TooLarge { max: 1200 },
            Error::Tls("x".into()),
            Error::Untrusted(Fingerprint::from_bytes([7; 32])),
            Error::Io(std::io::Error::other("x")),
            Error::Transport("x".into()),
        ];
        let codes: HashSet<c_int> = variants.iter().map(code).collect();
        assert_eq!(codes.len(), 24, "two variants share a code");
        assert!(codes.iter().all(|c| (1..=24).contains(c)), "{codes:?}");
        assert!(!codes.iter().any(|c| (100..=104).contains(c)), "{codes:?}");
    }
}
