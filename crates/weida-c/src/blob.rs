//! The piece tree of SWARM as values: blob ids, blobs, piece lists, the tree
//! hasher and bit maps (docs/c/swarm.md).

use std::ffi::c_int;
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use weida_rs::{Bits, Blob, BlobId, MAX_PIECES, PieceList, PieceSize, TreeHasher};

use crate::error::{
    WEIDA_EINVAL, closed, fail_abi, guard, guard_or, guard_ptr, guard_void, null_arg,
};
use crate::handle::{borrow, bytes, into_handle, out_ptr, release, write_out};

/// The bytes of a blob id (`BlobId`): a BLAKE3 hash.
pub const WEIDA_BLOB_ID_LEN: c_int = 32;
const _: () = assert!(WEIDA_BLOB_ID_LEN as usize == std::mem::size_of::<BlobId>());
/// The most pieces a blob has (`MAX_PIECES`).
pub const WEIDA_MAX_PIECES: c_int = 1_048_576;
const _: () = assert!(WEIDA_MAX_PIECES as u32 == MAX_PIECES);
/// `PieceSize::MIN` as its base-2 logarithm: 1 KiB.
pub const WEIDA_PIECE_SIZE_MIN_LOG2: c_int = 10;
const _: () = assert!(WEIDA_PIECE_SIZE_MIN_LOG2 as u8 == PieceSize::MIN.log2());
/// `PieceSize::MAX` as its base-2 logarithm: 16 MiB.
pub const WEIDA_PIECE_SIZE_MAX_LOG2: c_int = 24;
const _: () = assert!(WEIDA_PIECE_SIZE_MAX_LOG2 as u8 == PieceSize::MAX.log2());
/// `PieceSize::DEFAULT` as its base-2 logarithm: 1 MiB.
pub const WEIDA_PIECE_SIZE_DEFAULT_LOG2: c_int = 20;
const _: () = assert!(WEIDA_PIECE_SIZE_DEFAULT_LOG2 as u8 == PieceSize::DEFAULT.log2());

/// A blob: its root, size and piece size (`Blob`), read-only.
pub struct weida_blob {
    blob: Blob,
}

impl weida_blob {
    pub(crate) fn handle(blob: Blob) -> *mut weida_blob {
        into_handle(weida_blob { blob })
    }

    pub(crate) fn get(&self) -> Blob {
        self.blob
    }
}

/// The piece hashes of a blob, checked against its root (`PieceList`),
/// read-only.
pub struct weida_piece_list {
    pub(crate) list: Arc<PieceList>,
    /// The bytes `weida_piece_list_to_bytes` lends, made on its first call.
    bytes: OnceLock<Box<[u8]>>,
}

impl weida_piece_list {
    pub(crate) fn handle(list: Arc<PieceList>) -> *mut weida_piece_list {
        into_handle(weida_piece_list {
            list,
            bytes: OnceLock::new(),
        })
    }
}

/// The one-pass builder of a piece list (`TreeHasher`); `finish` takes the
/// value out.
pub struct weida_tree_hasher {
    hasher: Mutex<Option<TreeHasher>>,
}

/// A map of held pieces (`Bits`). Its calls take the lock in turn.
pub struct weida_bits {
    bits: Mutex<Bits>,
}

impl weida_bits {
    pub(crate) fn handle(bits: Bits) -> *mut weida_bits {
        into_handle(weida_bits {
            bits: Mutex::new(bits),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Bits> {
        self.bits.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A copy of the map, taken by the call that hands it on.
    pub(crate) fn get(&self) -> Bits {
        self.lock().clone()
    }
}

/// A piece size in, as its base-2 logarithm.
fn piece_size(log2: u8) -> Result<PieceSize, c_int> {
    PieceSize::from_log2(log2)
        .ok_or_else(|| fail_abi(WEIDA_EINVAL, c"a piece size is 2^10 to 2^24 bytes"))
}

/// A blob id in: `WEIDA_BLOB_ID_LEN` bytes.
///
/// # Safety
///
/// `root` is NULL or points to `WEIDA_BLOB_ID_LEN` readable bytes.
unsafe fn blob_id(root: *const u8) -> Result<BlobId, c_int> {
    if root.is_null() {
        return Err(null_arg());
    }
    let mut id = [0; WEIDA_BLOB_ID_LEN as usize];
    // SAFETY: by the caller's contract `root` points to that many bytes.
    id.copy_from_slice(unsafe { std::slice::from_raw_parts(root, WEIDA_BLOB_ID_LEN as usize) });
    Ok(BlobId::from_bytes(id))
}

/// Writes a blob id out.
///
/// # Safety
///
/// `out` passed `out_ptr` and points to `WEIDA_BLOB_ID_LEN` writable bytes.
unsafe fn write_id(out: *mut u8, id: BlobId) {
    let id = id.as_bytes();
    // SAFETY: by the caller's contract `out` has room for the id, and a
    // caller's buffer cannot overlap the id on this stack.
    unsafe { ptr::copy_nonoverlapping(id.as_ptr(), out, id.len()) };
}

// --- weida_blob_id ----------------------------------------------------------

/// The id of `len` bytes (`BlobId::of`): their BLAKE3 hash,
/// `WEIDA_BLOB_ID_LEN` bytes written to `out`.
///
/// # Safety
///
/// `data` is NULL only with `len == 0`, else points to `len` readable bytes;
/// `out` is NULL or points to `WEIDA_BLOB_ID_LEN` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_blob_id_of(data: *const u8, len: usize, out: *mut u8) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let data = unsafe { bytes(data, len) }?;
        // SAFETY: `out` passed `out_ptr` and has room for an id.
        unsafe { write_id(out, BlobId::of(data)) };
        Ok(0)
    })
}

// --- weida_blob -------------------------------------------------------------

/// A blob of `size` bytes whose hash is `root` (`WEIDA_BLOB_ID_LEN` bytes),
/// cut into pieces of `2^piece_log2` bytes (`Blob::new`). A `piece_log2`
/// outside 10 to 24, more than `WEIDA_MAX_PIECES` pieces, or an empty blob
/// whose root is not the hash of no bytes is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `root` is NULL or points to `WEIDA_BLOB_ID_LEN` readable bytes; `out` is
/// NULL or writable. Release the result with `weida_blob_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_blob_new(
    root: *const u8,
    size: u64,
    piece_log2: u8,
    out: *mut *mut weida_blob,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let root = unsafe { blob_id(root) }?;
        let blob = Blob::new(root, size, piece_size(piece_log2)?).ok_or_else(|| {
            fail_abi(
                WEIDA_EINVAL,
                c"a blob has at most WEIDA_MAX_PIECES pieces, and an empty one the root of no bytes",
            )
        })?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_blob::handle(blob)) };
        Ok(0)
    })
}

/// Writes the blob's root (`Blob::root`), `WEIDA_BLOB_ID_LEN` bytes, to
/// `out`.
///
/// # Safety
///
/// `b` is NULL or a live blob; `out` is NULL or points to
/// `WEIDA_BLOB_ID_LEN` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_blob_root(b: *const weida_blob, out: *mut u8) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let b = unsafe { borrow(b) }?;
        // SAFETY: `out` passed `out_ptr` and has room for an id.
        unsafe { write_id(out, b.blob.root()) };
        Ok(0)
    })
}

/// The blob's length in bytes (`Blob::size`); 0 for NULL.
///
/// # Safety
///
/// `b` is NULL or a live blob.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_blob_size(b: *const weida_blob) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { blob_number(b, |b| b.size()) }
}

/// The base-2 logarithm of the blob's piece size (`Blob::piece`), 10 to 24;
/// 0 for NULL.
///
/// # Safety
///
/// `b` is NULL or a live blob.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_blob_piece_log2(b: *const weida_blob) -> u8 {
    // SAFETY: forwarded from this function's contract.
    unsafe { blob_number(b, |b| b.piece().log2()) }
}

/// The number of pieces (`Blob::count`): the size over the piece size,
/// rounded up; 0 for an empty blob and for NULL.
///
/// # Safety
///
/// `b` is NULL or a live blob.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_blob_count(b: *const weida_blob) -> u32 {
    // SAFETY: forwarded from this function's contract.
    unsafe { blob_number(b, |b| b.count()) }
}

/// A number of a blob; 0 for NULL.
///
/// # Safety
///
/// `b` is NULL or a live blob.
unsafe fn blob_number<T: Default>(b: *const weida_blob, f: impl FnOnce(&Blob) -> T) -> T {
    guard_or(T::default(), || {
        if b.is_null() {
            return T::default();
        }
        // SAFETY: a non-NULL `b` is a live blob by the caller's contract.
        f(&unsafe { &*b }.blob)
    })
}

/// The byte range of piece `index` (`Blob::span`): 1 with its first byte in
/// `*out_start` and the byte after its last in `*out_end`; the last piece is
/// shorter when the size is not a multiple of the piece size. 0 past the last
/// piece and for any NULL argument.
///
/// # Safety
///
/// `b` is NULL or a live blob; `out_start` and `out_end` are NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_blob_span(
    b: *const weida_blob,
    index: u32,
    out_start: *mut u64,
    out_end: *mut u64,
) -> c_int {
    guard_or(0, || {
        if b.is_null() || out_start.is_null() || out_end.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `b` is a live blob by this function's contract.
        match unsafe { &*b }.blob.span(index) {
            Some(span) => {
                // SAFETY: non-NULL outs are writable by this function's
                // contract.
                unsafe {
                    out_start.write(span.start);
                    out_end.write(span.end);
                }
                1
            }
            None => 0,
        }
    })
}

/// Releases a blob. NULL does nothing.
///
/// # Safety
///
/// `b` is NULL or a live blob, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_blob_drop(b: *mut weida_blob) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(b) }));
}

// --- weida_piece_list -------------------------------------------------------

/// The piece list of `len` bytes cut into pieces of `2^piece_log2` bytes
/// (`PieceList::of`); its blob is `weida_piece_list_blob`. A `piece_log2`
/// outside 10 to 24 or more than `WEIDA_MAX_PIECES` pieces is
/// `-WEIDA_EINVAL`. Runs on the caller's thread.
///
/// # Safety
///
/// `data` is NULL only with `len == 0`, else points to `len` readable bytes;
/// `out` is NULL or writable. Release the result with
/// `weida_piece_list_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_list_of(
    data: *const u8,
    len: usize,
    piece_log2: u8,
    out: *mut *mut weida_piece_list,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let data = unsafe { bytes(data, len) }?;
        let list = PieceList::of(data, piece_size(piece_log2)?).ok_or_else(too_many)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_piece_list::handle(Arc::new(list))) };
        Ok(0)
    })
}

/// The piece list of `blob` from its bytes (`PieceList::parse`), as
/// `weida_piece_list_to_bytes` gives them: `-WEIDA_EINVAL` unless they are
/// one chaining value per piece that hash to the blob's root.
///
/// # Safety
///
/// `blob` is NULL or a live blob; `data` is NULL only with `len == 0`, else
/// points to `len` readable bytes; `out` is NULL or writable. Release the
/// result with `weida_piece_list_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_list_parse(
    blob: *const weida_blob,
    data: *const u8,
    len: usize,
    out: *mut *mut weida_piece_list,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let blob = unsafe { borrow(blob) }?;
        // SAFETY: forwarded from this function's contract.
        let data = unsafe { bytes(data, len) }?;
        let list = PieceList::parse(blob.blob, data).ok_or_else(|| {
            fail_abi(
                WEIDA_EINVAL,
                c"the bytes are not the piece list of this blob",
            )
        })?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_piece_list::handle(Arc::new(list))) };
        Ok(0)
    })
}

/// The blob this list belongs to (`PieceList::blob`), as a new handle.
///
/// # Safety
///
/// `l` is NULL or a live piece list; `out` is NULL or writable. Release the
/// result with `weida_blob_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_list_blob(
    l: *const weida_piece_list,
    out: *mut *mut weida_blob,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_blob::handle(l.list.blob())) };
        Ok(0)
    })
}

/// The list's bytes (`PieceList::to_bytes`): the chaining values one after
/// the other, 32 bytes each, empty for a blob of at most one piece; their
/// length in `*len_out` when that is not NULL. NULL for NULL. Valid as long
/// as `l`.
///
/// # Safety
///
/// `l` is NULL or a live piece list; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_list_to_bytes(
    l: *const weida_piece_list,
    len_out: *mut usize,
) -> *const u8 {
    guard_ptr(|| {
        if l.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `l` is live by this function's contract; the
        // bytes live as long as it.
        let l = unsafe { &*l };
        let bytes = l.bytes.get_or_init(|| l.list.to_bytes().into());
        if !len_out.is_null() {
            // SAFETY: a non-NULL `len_out` is writable by this function's
            // contract.
            unsafe { len_out.write(bytes.len()) };
        }
        bytes.as_ptr()
    })
}

/// Whether `len` bytes are piece `index` of the list's blob
/// (`PieceList::check`): 1 when they hash to its slot in the list, 0
/// otherwise, also past the last piece.
///
/// # Safety
///
/// `l` is NULL or a live piece list; `piece` is NULL only with `len == 0`,
/// else points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_list_check(
    l: *const weida_piece_list,
    index: u32,
    piece: *const u8,
    len: usize,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let piece = unsafe { bytes(piece, len) }?;
        Ok(c_int::from(l.list.check(index, piece)))
    })
}

/// Releases a piece list. NULL does nothing.
///
/// # Safety
///
/// `l` is NULL or a live piece list, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_list_drop(l: *mut weida_piece_list) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(l) }));
}

fn too_many() -> c_int {
    fail_abi(WEIDA_EINVAL, c"more than WEIDA_MAX_PIECES pieces")
}

// --- weida_tree_hasher ------------------------------------------------------

/// A tree hasher that cuts a blob into pieces of `2^piece_log2` bytes
/// (`TreeHasher::new`): fed the bytes in order, it yields the root and the
/// piece list in one pass. A `piece_log2` outside 10 to 24 is
/// `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_tree_hasher_finish` or `weida_tree_hasher_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_tree_hasher_new(
    piece_log2: u8,
    out: *mut *mut weida_tree_hasher,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        let hasher = TreeHasher::new(piece_size(piece_log2)?);
        let handle = into_handle(weida_tree_hasher {
            hasher: Mutex::new(Some(hasher)),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Feeds the next `len` bytes of the blob (`TreeHasher::update`). Runs on
/// the caller's thread; calls from several threads take turns, in an order
/// the caller does not control.
///
/// # Safety
///
/// `h` is NULL or a live tree hasher; `data` is NULL only with `len == 0`,
/// else points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_tree_hasher_update(
    h: *const weida_tree_hasher,
    data: *const u8,
    len: usize,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let h = unsafe { borrow(h) }?;
        // SAFETY: forwarded from this function's contract.
        let data = unsafe { bytes(data, len) }?;
        h.hasher
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
            .ok_or_else(closed)?
            .update(data);
        Ok(0)
    })
}

/// Ends the bytes and writes their piece list to `out`
/// (`TreeHasher::finish`); its blob holds the root. More than
/// `WEIDA_MAX_PIECES` pieces is `-WEIDA_EINVAL`. **Consumes** `h` on every
/// return path.
///
/// # Safety
///
/// `h` is NULL or a live tree hasher, not used again; `out` is NULL or
/// writable. Release the result with `weida_piece_list_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_tree_hasher_finish(
    h: *mut weida_tree_hasher,
    out: *mut *mut weida_piece_list,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let h = unsafe { release(h) }.ok_or_else(null_arg)?;
        let out = out_ptr(out)?;
        let hasher = h
            .hasher
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(closed)?;
        let list = hasher.finish().ok_or_else(too_many)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_piece_list::handle(Arc::new(list))) };
        Ok(0)
    })
}

/// Releases a tree hasher unfinished. NULL does nothing.
///
/// # Safety
///
/// `h` is NULL or a live tree hasher, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_tree_hasher_drop(h: *mut weida_tree_hasher) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(h) }));
}

// --- weida_bits -------------------------------------------------------------

/// A map of `len` bits, all clear (`Bits::new`).
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with `weida_bits_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_new(len: u32, out: *mut *mut weida_bits) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { bits_out(out, || Ok(Bits::new(len))) }
}

/// A map of `len` bits, all set (`Bits::full`).
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with `weida_bits_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_full(len: u32, out: *mut *mut weida_bits) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { bits_out(out, || Ok(Bits::full(len))) }
}

/// A map of `len` bits from the `n` bytes they travel as
/// (`Bits::from_bytes`): bit `i` is bit `i % 8`, least significant first, of
/// byte `i / 8`, in `(len + 7) / 8` bytes whose padding bits are clear.
/// Anything else is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `data` is NULL only with `n == 0`, else points to `n` readable bytes;
/// `out` is NULL or writable. Release the result with `weida_bits_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_from_bytes(
    len: u32,
    data: *const u8,
    n: usize,
    out: *mut *mut weida_bits,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        bits_out(out, || {
            let data = bytes(data, n)?;
            Bits::from_bytes(len, data).ok_or_else(|| {
                fail_abi(WEIDA_EINVAL, c"the bytes are not a bit map of this length")
            })
        })
    }
}

/// Writes a new map from `make` to `out`.
///
/// # Safety
///
/// `out` is NULL or writable.
unsafe fn bits_out(out: *mut *mut weida_bits, make: impl FnOnce() -> Result<Bits, c_int>) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        let handle = weida_bits::handle(make()?);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Copies the map's bytes (`Bits::as_bytes`), `(len + 7) / 8` of them laid
/// out as for `weida_bits_from_bytes`, into `buf` and writes their count to
/// `*out_n`. A `cap` below that count is `-WEIDA_EINVAL`; `buf` may be NULL
/// only for a map of no bytes.
///
/// # Safety
///
/// `b` is NULL or a live map; `buf` is NULL or points to `cap` writable
/// bytes; `out_n` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_as_bytes(
    b: *const weida_bits,
    buf: *mut u8,
    cap: usize,
    out_n: *mut usize,
) -> c_int {
    guard(|| {
        let out_n = out_ptr(out_n)?;
        // SAFETY: forwarded from this function's contract.
        let b = unsafe { borrow(b) }?;
        let bits = b.lock();
        let bytes = bits.as_bytes();
        if !bytes.is_empty() {
            if buf.is_null() {
                return Err(null_arg());
            }
            if cap < bytes.len() {
                return Err(fail_abi(
                    WEIDA_EINVAL,
                    c"the buffer is shorter than the bit map",
                ));
            }
            // SAFETY: `buf` is non-NULL and has room for `cap` bytes, at
            // least as many as the map's, by this function's contract.
            unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), buf, bytes.len()) };
        }
        // SAFETY: `out_n` passed `out_ptr`.
        unsafe { write_out(out_n, bytes.len()) };
        Ok(0)
    })
}

/// How many bits the map has (`Bits::len`); 0 for NULL.
///
/// # Safety
///
/// `b` is NULL or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_len(b: *const weida_bits) -> u32 {
    // SAFETY: forwarded from this function's contract.
    unsafe { bits_number(b, |b| b.len()) }
}

/// How many bits are set (`Bits::ones`); 0 for NULL.
///
/// # Safety
///
/// `b` is NULL or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_ones(b: *const weida_bits) -> u32 {
    // SAFETY: forwarded from this function's contract.
    unsafe { bits_number(b, |b| b.ones()) }
}

/// Whether bit `index` is set (`Bits::get`): 1 or 0; 0 past the end and for
/// NULL.
///
/// # Safety
///
/// `b` is NULL or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_get(b: *const weida_bits, index: u32) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { bits_number(b, |b| c_int::from(b.get(index))) }
}

/// Whether every bit is set (`Bits::all`): 1 or 0; 1 for a map of no bits,
/// 0 for NULL.
///
/// # Safety
///
/// `b` is NULL or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_all(b: *const weida_bits) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { bits_number(b, |b| c_int::from(b.all())) }
}

/// A number of a map; 0 for NULL.
///
/// # Safety
///
/// `b` is NULL or a live map.
unsafe fn bits_number<T: Default>(b: *const weida_bits, f: impl FnOnce(&Bits) -> T) -> T {
    guard_or(T::default(), || {
        if b.is_null() {
            return T::default();
        }
        // SAFETY: a non-NULL `b` is a live map by the caller's contract.
        f(&unsafe { &*b }.lock())
    })
}

/// Sets bit `index` when `on` is non-zero, clears it otherwise (`Bits::set`;
/// a flag, R7). Past the end it does nothing.
///
/// # Safety
///
/// `b` is NULL or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_set(b: *const weida_bits, index: u32, on: c_int) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let b = unsafe { borrow(b) }?;
        b.lock().set(index, on != 0);
        Ok(0)
    })
}

/// Releases a map. NULL does nothing.
///
/// # Safety
///
/// `b` is NULL or a live map, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bits_drop(b: *mut weida_bits) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(b) }));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `17 * 1024 + 100` bytes: 18 pieces of 1 KiB, the last one short.
    fn data() -> Vec<u8> {
        (0..17 * 1024 + 100)
            .map(|i: u32| i.wrapping_mul(2_654_435_761).to_be_bytes()[0])
            .collect()
    }

    /// The bytes a piece list lends, copied.
    ///
    /// # Safety
    ///
    /// `l` is a live piece list.
    unsafe fn list_bytes(l: *const weida_piece_list) -> Vec<u8> {
        let mut len = 0;
        // SAFETY: by this function's contract `l` is live; the bytes are
        // copied before it can go.
        unsafe {
            let p = weida_piece_list_to_bytes(l, &mut len);
            assert!(!p.is_null());
            std::slice::from_raw_parts(p, len).to_vec()
        }
    }

    #[test]
    fn a_tree_hashers_list_checks_every_piece_and_matches_the_root() {
        let data = data();
        let (mut h, mut list, mut of, mut b) = (
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
        );
        let mut root = [0u8; 32];
        let mut id = [0u8; 32];
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable, every buffer as long as passed.
        unsafe {
            assert_eq!(weida_tree_hasher_new(10, &mut h), 0);
            assert_eq!(weida_tree_hasher_update(h, data.as_ptr(), 5000), 0);
            assert_eq!(
                weida_tree_hasher_update(h, data[5000..].as_ptr(), data.len() - 5000),
                0
            );
            assert_eq!(weida_tree_hasher_finish(h, &mut list), 0);
            assert_eq!(
                weida_piece_list_of(data.as_ptr(), data.len(), 10, &mut of),
                0
            );
            assert_eq!(list_bytes(list), list_bytes(of));
            assert_eq!(list_bytes(list).len(), 18 * 32);

            assert_eq!(weida_piece_list_blob(list, &mut b), 0);
            assert_eq!(weida_blob_count(b), 18);
            assert_eq!(weida_blob_size(b), 17 * 1024 + 100);
            assert_eq!(weida_blob_piece_log2(b), 10);
            assert_eq!(weida_blob_root(b, root.as_mut_ptr()), 0);
            assert_eq!(
                weida_blob_id_of(data.as_ptr(), data.len(), id.as_mut_ptr()),
                0
            );
            assert_eq!(root, id);

            let (mut start, mut end) = (0, 0);
            assert_eq!(weida_blob_span(b, 17, &mut start, &mut end), 1);
            assert_eq!((start, end), (17408, 17508));
            assert_eq!(weida_blob_span(b, 18, &mut start, &mut end), 0);

            for index in 0..18u32 {
                assert_eq!(weida_blob_span(b, index, &mut start, &mut end), 1);
                let piece = &data[start as usize..end as usize];
                assert_eq!(
                    weida_piece_list_check(list, index, piece.as_ptr(), piece.len()),
                    1,
                    "piece {index}"
                );
            }
            let mut flipped = data[..1024].to_vec();
            flipped[7] ^= 1;
            assert_eq!(
                weida_piece_list_check(list, 0, flipped.as_ptr(), flipped.len()),
                0
            );

            let bytes = list_bytes(list);
            let mut parsed = ptr::null_mut();
            assert_eq!(
                weida_piece_list_parse(b, bytes.as_ptr(), bytes.len(), &mut parsed),
                0
            );
            assert_eq!(list_bytes(parsed), bytes);
            weida_piece_list_drop(parsed);
            assert_eq!(
                weida_piece_list_parse(b, bytes.as_ptr(), bytes.len() - 32, &mut parsed),
                -WEIDA_EINVAL
            );

            let mut other = ptr::null_mut();
            assert_eq!(
                weida_blob_new(root.as_ptr(), 1, 9, &mut other),
                -WEIDA_EINVAL
            );
            assert_eq!(
                weida_blob_new(root.as_ptr(), 1, 25, &mut other),
                -WEIDA_EINVAL
            );
            assert_eq!(
                weida_blob_new(root.as_ptr(), 0, 10, &mut other),
                -WEIDA_EINVAL
            );
            assert_eq!(
                weida_blob_new(root.as_ptr(), data.len() as u64, 10, &mut other),
                0
            );
            assert_eq!(weida_blob_count(other), 18);
            weida_blob_drop(other);

            weida_blob_drop(b);
            weida_piece_list_drop(of);
            weida_piece_list_drop(list);
        }
    }

    #[test]
    fn bits_travel_least_significant_bit_first() {
        let (mut b, mut back, mut full) = (ptr::null_mut(), ptr::null_mut(), ptr::null_mut());
        let mut buf = [0u8; 2];
        let mut n = 0;
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable, every buffer as long as passed.
        unsafe {
            assert_eq!(weida_bits_new(10, &mut b), 0);
            assert_eq!(weida_bits_set(b, 0, 1), 0);
            assert_eq!(weida_bits_set(b, 9, 1), 0);
            assert_eq!(weida_bits_set(b, 10, 1), 0);
            assert_eq!(weida_bits_get(b, 0), 1);
            assert_eq!(weida_bits_get(b, 1), 0);
            assert_eq!(weida_bits_get(b, 10), 0);
            assert_eq!(weida_bits_len(b), 10);
            assert_eq!(weida_bits_ones(b), 2);
            assert_eq!(weida_bits_all(b), 0);
            assert_eq!(weida_bits_as_bytes(b, buf.as_mut_ptr(), 2, &mut n), 0);
            assert_eq!((buf, n), ([0x01, 0x02], 2));
            assert_eq!(
                weida_bits_as_bytes(b, buf.as_mut_ptr(), 1, &mut n),
                -WEIDA_EINVAL
            );

            assert_eq!(
                weida_bits_from_bytes(10, [0x01, 0x02].as_ptr(), 2, &mut back),
                0
            );
            assert_eq!(weida_bits_get(back, 0), 1);
            assert_eq!(weida_bits_get(back, 9), 1);
            assert_eq!(weida_bits_ones(back), 2);
            let mut bad = ptr::null_mut();
            assert_eq!(
                weida_bits_from_bytes(10, [0x01, 0x06].as_ptr(), 2, &mut bad),
                -WEIDA_EINVAL
            );
            assert_eq!(
                weida_bits_from_bytes(10, [0x01].as_ptr(), 1, &mut bad),
                -WEIDA_EINVAL
            );

            assert_eq!(weida_bits_full(10, &mut full), 0);
            assert_eq!(weida_bits_all(full), 1);
            assert_eq!(weida_bits_ones(full), 10);

            weida_bits_drop(full);
            weida_bits_drop(back);
            weida_bits_drop(b);
        }
    }
}
