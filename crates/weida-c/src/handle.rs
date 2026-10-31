//! Handle pointers and the pointer arguments every export validates
//! (docs/c/README.md R5, R7).

use std::ffi::{CStr, c_char, c_int};
use std::ptr;
use std::sync::Arc;

use crate::error::{WEIDA_EINVAL, fail_abi, null_arg};

/// Hands `value` to C as a new handle: `Arc::into_raw` of a fresh `Arc`.
pub(crate) fn into_handle<T>(value: T) -> *mut T {
    share(Arc::new(value))
}

/// Hands one more reference of an existing handle to C.
pub(crate) fn share<T>(value: Arc<T>) -> *mut T {
    Arc::into_raw(value).cast_mut()
}

/// A reference of the handle `h` for the duration of one call.
///
/// # Safety
///
/// `h` is NULL or a pointer this library handed out for a `T` whose `_drop`
/// has not returned.
pub(crate) unsafe fn borrow<T>(h: *const T) -> Result<Arc<T>, c_int> {
    if h.is_null() {
        return Err(null_arg());
    }
    // SAFETY: by the caller's contract `h` came from `Arc::into_raw` and the
    // reference it stands for is still held, so the count is at least one.
    unsafe {
        Arc::increment_strong_count(h);
        Ok(Arc::from_raw(h))
    }
}

/// Gives back the reference the caller held; NULL does nothing.
///
/// # Safety
///
/// `h` is NULL or a pointer this library handed out for a `T`, released at
/// most once.
pub(crate) unsafe fn release<T>(h: *const T) -> Option<Arc<T>> {
    if h.is_null() {
        return None;
    }
    // SAFETY: by the caller's contract this is the one release of the
    // reference `h` stands for.
    Some(unsafe { Arc::from_raw(h) })
}

/// A NUL-terminated UTF-8 argument.
///
/// # Safety
///
/// `s` is NULL or points to a NUL-terminated string that stays valid for
/// `'a`.
pub(crate) unsafe fn cstr<'a>(s: *const c_char) -> Result<&'a str, c_int> {
    if s.is_null() {
        return Err(null_arg());
    }
    // SAFETY: by the caller's contract `s` is a live NUL-terminated string.
    let text = unsafe { CStr::from_ptr(s) };
    text.to_str()
        .map_err(|_| fail_abi(WEIDA_EINVAL, c"a string argument was not UTF-8"))
}

/// A byte-buffer argument; `data` may be NULL only when `len` is 0.
///
/// # Safety
///
/// `data` is NULL or points to `len` readable bytes that stay valid for
/// `'a`.
pub(crate) unsafe fn bytes<'a>(data: *const u8, len: usize) -> Result<&'a [u8], c_int> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(null_arg());
    }
    // SAFETY: by the caller's contract `data` points to `len` readable bytes.
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

/// Checks an out-parameter before any work is done, so a failure leaks
/// nothing.
pub(crate) fn out_ptr<T>(out: *mut T) -> Result<*mut T, c_int> {
    if out.is_null() {
        Err(null_arg())
    } else {
        Ok(out)
    }
}

/// Writes an output checked by [`out_ptr`].
///
/// # Safety
///
/// `out` passed [`out_ptr`] and points to writable memory for a `T`.
pub(crate) unsafe fn write_out<T>(out: *mut T, value: T) {
    // SAFETY: by the caller's contract `out` is non-NULL and writable.
    unsafe { out.write(value) }
}

/// Text a handle hands out: the bytes plus a NUL, so an interior NUL is
/// representable and the length is exact (R7).
pub(crate) struct CText(Box<[u8]>);

impl CText {
    pub(crate) fn new(text: impl Into<Vec<u8>>) -> CText {
        let mut bytes = text.into();
        bytes.push(0);
        CText(bytes.into_boxed_slice())
    }

    pub(crate) fn as_ptr(&self) -> *const c_char {
        self.0.as_ptr().cast()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len() - 1
    }
}

/// Hands `text` out as a string getter does: its pointer, and its length
/// through `len_out` when that is not NULL; NULL when absent.
///
/// # Safety
///
/// `len_out` is NULL or writable.
pub(crate) unsafe fn text_out(text: Option<&CText>, len_out: *mut usize) -> *const c_char {
    let Some(text) = text else {
        return ptr::null();
    };
    if !len_out.is_null() {
        // SAFETY: by the caller's contract a non-NULL `len_out` is writable.
        unsafe { len_out.write(text.len()) };
    }
    text.as_ptr()
}
