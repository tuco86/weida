//! One segment of a radio (and later of a raw peer): layers written without
//! waiting (docs/c/radio.md).

use std::ffi::{c_char, c_int};
use std::ptr;
use std::sync::{Mutex, PoisonError};

use weida_rs::Segment;

use crate::error::{closed, fail, guard, guard_or, guard_ptr, guard_void, null_arg};
use crate::handle::{CText, borrow, bytes, into_handle, out_ptr, release, text_out, write_out};

/// How many layers a segment carries: 0 to 15 (`MAX_SEGMENT_LAYERS`).
pub const WEIDA_MAX_SEGMENT_LAYERS: c_int = 16;

/// One open segment (`Segment`). Its calls take the lock in turn, where
/// Rust's `&mut self` forbids concurrent use; `weida_segment_finish` takes
/// the value out.
pub struct weida_segment {
    segment: Mutex<Option<Segment>>,
    topic: CText,
    number: u64,
}

impl weida_segment {
    pub(crate) fn handle(segment: Segment) -> *mut weida_segment {
        // Asserted at compile time, here where cbindgen does not look.
        const { assert!(WEIDA_MAX_SEGMENT_LAYERS == weida_rs::MAX_SEGMENT_LAYERS as c_int) };
        into_handle(weida_segment {
            topic: CText::new(segment.topic()),
            number: segment.number(),
            segment: Mutex::new(Some(segment)),
        })
    }
}

/// Writes one chunk to `layer` of every copy still taking it and writes how
/// many took it.
///
/// # Safety
///
/// As `weida_segment_write_layer`.
unsafe fn write(
    s: *const weida_segment,
    layer: u8,
    chunk: *const u8,
    len: usize,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let chunk = unsafe { bytes(chunk, len) }?.to_vec();
        let took = s
            .segment
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .ok_or_else(closed)?
            .write_layer(layer, chunk)
            .map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, took) };
        Ok(0)
    })
}

/// This segment's number on its topic (`Segment::number`); the first
/// segment on a topic is 0. 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live segment.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_number(s: *const weida_segment) -> u64 {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live segment by this function's
        // contract.
        unsafe { &*s }.number
    })
}

/// The topic this segment belongs to (`Segment::topic`). Lives as long as
/// the segment.
///
/// # Safety
///
/// `s` is NULL or a live segment; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_topic(
    s: *const weida_segment,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if s.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `s` is a live segment by this function's
        // contract; the text lives as long as it.
        let s = unsafe { &*s };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&s.topic), len_out) }
    })
}

/// Writes a chunk to layer 0 (`Segment::write`); as
/// `weida_segment_write_layer`.
///
/// # Safety
///
/// `s` is NULL or a live segment; `chunk` is NULL only with `len == 0`,
/// else points to `len` readable bytes; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_write(
    s: *const weida_segment,
    chunk: *const u8,
    len: usize,
    out: *mut usize,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { write(s, 0, chunk, len, out) }
}

/// Writes a chunk to `layer` of every copy still taking it, without ever
/// waiting (`Segment::write_layer`), and writes how many copies took it.
/// The chunk is copied. A copy with no room loses this layer and every
/// layer above it, and the whole segment when the layer is 0; a copy
/// capped below the layer by its dish's `max_layer` is skipped, which is
/// not a drop.
///
/// Fails with `-WEIDA_ELIMIT_EXCEEDED` for a layer of
/// `WEIDA_MAX_SEGMENT_LAYERS` or above or a chunk above 4 GiB, and with
/// `-WEIDA_ERUNTIME` for a layer already finished.
///
/// # Safety
///
/// `s` is NULL or a live segment; `chunk` is NULL only with `len == 0`,
/// else points to `len` readable bytes; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_write_layer(
    s: *const weida_segment,
    layer: u8,
    chunk: *const u8,
    len: usize,
    out: *mut usize,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { write(s, layer, chunk, len, out) }
}

/// Finishes `layer` on every copy (`Segment::finish_layer`); a later write
/// to it is `-WEIDA_ERUNTIME`. A layer of `WEIDA_MAX_SEGMENT_LAYERS` or
/// above is ignored, as in Rust.
///
/// # Safety
///
/// `s` is NULL or a live segment.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_finish_layer(s: *const weida_segment, layer: u8) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        s.segment
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .ok_or_else(closed)?
            .finish_layer(layer);
        Ok(0)
    })
}

/// Finishes every layer still open (`Segment::finish`) and writes how many
/// copies still hold layer 0. **Consumes** `s` on every return path; a
/// NULL `out` fails with `-WEIDA_EINVAL` after the segment was dropped,
/// which resets every unfinished layer.
///
/// # Safety
///
/// `s` is NULL or a live segment, not used again; `out` is NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_finish(s: *mut weida_segment, out: *mut usize) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let s = unsafe { release(s) }.ok_or_else(null_arg)?;
        let segment = s
            .segment
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let out = out_ptr(out)?;
        let held = segment.ok_or_else(closed)?.finish();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, held) };
        Ok(0)
    })
}

/// Releases a segment without finishing it: every unfinished layer is
/// reset on every copy. NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live segment, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_drop(s: *mut weida_segment) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(s) }));
}
