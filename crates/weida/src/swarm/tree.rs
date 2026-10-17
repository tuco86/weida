//! The piece tree: how a blob is cut into pieces and how each piece is
//! checked against the blob's name.
//!
//! The name of a blob is the BLAKE3 hash of its bytes. BLAKE3 is a tree hash
//! (section 2.1 of the BLAKE3 paper,
//! <https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf>): a
//! piece is a power-of-two multiple of BLAKE3's 1 KiB chunks at a multiple of
//! its own size, so every piece is exactly one subtree of that tree, the last
//! one possibly short. Its chaining value is
//! `Hasher::new().set_input_offset(i * piece).update(bytes).finalize_non_root()`,
//! and merging the chaining values of all pieces the way the tree prescribes
//! (`left_subtree_len`, `merge_subtrees_non_root`, `merge_subtrees_root`, all
//! `blake3::hazmat`) gives the root. That is the construction of Bao
//! (<https://github.com/oconnor663/bao>) with the outboard cut at piece level:
//! a list of chaining values whose root is the blob id authenticates every
//! piece it lists, so a piece is checked the moment it arrives, in any order.
//!
//! A blob of one piece has no interior node: its one piece is checked against
//! the root directly, and its list is empty. A blob of no bytes is the hash of
//! the empty input.

use std::fmt;
use std::ops::Range;
use std::str::FromStr;

use blake3::hazmat::{
    ChainingValue, HasherExt, Mode, left_subtree_len, merge_subtrees_non_root, merge_subtrees_root,
};

/// The most pieces a blob may have: the piece list then takes 32 MiB and the
/// map of held pieces ([`Bits`]) 128 KiB.
pub const MAX_PIECES: u32 = 1 << 20;

/// Bytes one chaining value takes in a piece list.
const CV_LEN: usize = 32;

/// The 256-bit BLAKE3 hash of a blob, which names it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobId([u8; 32]);

impl BlobId {
    /// The id with these 32 bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> BlobId {
        BlobId(bytes)
    }

    /// The 32 bytes of the id.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The id of `bytes`: their BLAKE3 hash.
    pub fn of(bytes: &[u8]) -> BlobId {
        BlobId(*blake3::hash(bytes).as_bytes())
    }
}

impl fmt::Display for BlobId {
    /// 64 lowercase hex digits.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({self})")
    }
}

impl FromStr for BlobId {
    type Err = ParseBlobIdError;

    /// Exactly 64 hex digits, of either case.
    fn from_str(s: &str) -> Result<BlobId, ParseBlobIdError> {
        let digits = s.as_bytes();
        if digits.len() != 64 {
            return Err(ParseBlobIdError);
        }
        let nibble = |digit: u8| {
            char::from(digit)
                .to_digit(16)
                .and_then(|n| u8::try_from(n).ok())
                .ok_or(ParseBlobIdError)
        };
        let mut id = [0u8; 32];
        let (pairs, _) = digits.as_chunks::<2>();
        for (byte, [high, low]) in id.iter_mut().zip(pairs) {
            *byte = (nibble(*high)? << 4) | nibble(*low)?;
        }
        Ok(BlobId(id))
    }
}

/// A string was not 64 hex digits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseBlobIdError;

impl fmt::Display for ParseBlobIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a blob id is 64 hex digits")
    }
}

impl std::error::Error for ParseBlobIdError {}

/// The size of a piece: a power of two from 1 KiB to 16 MiB.
///
/// At least 1 KiB because BLAKE3's chunks are 1 KiB, and a power of two
/// because a piece must be one whole subtree of the hash tree.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PieceSize(u8);

impl PieceSize {
    /// 1 KiB, the size of a BLAKE3 chunk.
    pub const MIN: PieceSize = PieceSize(10);
    /// 16 MiB.
    pub const MAX: PieceSize = PieceSize(24);
    /// 1 MiB.
    pub const DEFAULT: PieceSize = PieceSize(20);

    /// The piece size of `2^log2` bytes; `None` outside 10 to 24.
    pub const fn from_log2(log2: u8) -> Option<PieceSize> {
        if matches!(log2, 10..=24) {
            Some(PieceSize(log2))
        } else {
            None
        }
    }

    /// The piece size of `bytes` bytes; `None` unless it is a power of two
    /// from 1 KiB to 16 MiB.
    pub fn new(bytes: u64) -> Option<PieceSize> {
        if !bytes.is_power_of_two() {
            return None;
        }
        PieceSize::from_log2(u8::try_from(bytes.trailing_zeros()).ok()?)
    }

    /// The base-2 logarithm of the size in bytes, 10 to 24.
    pub const fn log2(self) -> u8 {
        self.0
    }

    /// The size in bytes.
    pub const fn bytes(self) -> u64 {
        1 << self.0
    }
}

impl Default for PieceSize {
    fn default() -> PieceSize {
        PieceSize::DEFAULT
    }
}

/// What names a blob and how it is cut: the root hash, the size in bytes and
/// the piece size.
///
/// This is the analogue of a torrent file, without the piece hashes: those
/// travel as a [`PieceList`], and are checked against [`Blob::root`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Blob {
    root: BlobId,
    size: u64,
    piece: PieceSize,
}

impl Blob {
    /// The blob with this root, size and piece size.
    ///
    /// `None` when it would have more than [`MAX_PIECES`] pieces, or when it
    /// is empty and `root` is not the hash of the empty input.
    pub fn new(root: BlobId, size: u64, piece: PieceSize) -> Option<Blob> {
        if size.div_ceil(piece.bytes()) > u64::from(MAX_PIECES) {
            return None;
        }
        if size == 0 && root != BlobId::of(b"") {
            return None;
        }
        Some(Blob { root, size, piece })
    }

    /// The hash of the whole blob.
    pub fn root(&self) -> BlobId {
        self.root
    }

    /// The length of the blob in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The size of one piece.
    pub fn piece(&self) -> PieceSize {
        self.piece
    }

    /// How many pieces the blob has: its size divided by the piece size,
    /// rounded up; 0 for an empty blob.
    pub fn count(&self) -> u32 {
        // `new` caps the quotient at `MAX_PIECES`.
        u32::try_from(self.size.div_ceil(self.piece.bytes())).unwrap_or(MAX_PIECES)
    }

    /// The byte range of piece `index`; the last piece is short when the size
    /// is not a multiple of the piece size. `None` past the last piece.
    pub fn span(&self, index: u32) -> Option<Range<u64>> {
        if index >= self.count() {
            return None;
        }
        let start = u64::from(index) * self.piece.bytes();
        Some(start..(start + self.piece.bytes()).min(self.size))
    }
}

/// The chaining value of piece `index`, whose bytes are `bytes`: not empty and
/// at most one piece.
fn chaining_value(index: u32, bytes: &[u8], piece: PieceSize) -> ChainingValue {
    let mut hasher = blake3::Hasher::new();
    hasher.set_input_offset(u64::from(index) * piece.bytes());
    hasher.update(bytes);
    hasher.finalize_non_root()
}

/// The chaining values of the two halves of the tree over `cvs`, which hold
/// `len` bytes. `cvs` has at least two values.
fn halves(cvs: &[ChainingValue], len: u64, piece: PieceSize) -> (ChainingValue, ChainingValue) {
    let left = left_subtree_len(len);
    let split = usize::try_from(left / piece.bytes())
        .unwrap_or(usize::MAX)
        .min(cvs.len());
    (
        merge(&cvs[..split], left, piece),
        merge(&cvs[split..], len - left, piece),
    )
}

/// The chaining value of the subtree over `cvs`, which hold `len` bytes.
fn merge(cvs: &[ChainingValue], len: u64, piece: PieceSize) -> ChainingValue {
    if cvs.len() <= 1 {
        return cvs.first().copied().unwrap_or_default();
    }
    let (left, right) = halves(cvs, len, piece);
    merge_subtrees_non_root(&left, &right, Mode::Hash)
}

/// The root of the tree over `cvs`, which hold `len` bytes. `cvs` has at
/// least two values.
fn root_of(cvs: &[ChainingValue], len: u64, piece: PieceSize) -> BlobId {
    let (left, right) = halves(cvs, len, piece);
    BlobId(*merge_subtrees_root(&left, &right, Mode::Hash).as_bytes())
}

/// The chaining value of every piece of a blob, which authenticates each
/// piece against [`Blob::root`].
///
/// The list is empty when the blob has at most one piece: that piece is
/// checked against the root directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PieceList {
    blob: Blob,
    cvs: Vec<ChainingValue>,
}

impl PieceList {
    /// The blob `bytes` and the list of its pieces; `None` when it would have
    /// more than [`MAX_PIECES`] pieces.
    pub fn of(bytes: &[u8], piece: PieceSize) -> Option<PieceList> {
        let mut hasher = TreeHasher::new(piece);
        hasher.update(bytes);
        hasher.finish()
    }

    /// The list that `bytes` encodes for `blob`, as [`PieceList::to_bytes`]
    /// writes it. `None` when the length is not one value per piece (none at
    /// all for a blob of at most one piece), or when the values do not merge
    /// to [`Blob::root`].
    pub fn parse(blob: Blob, bytes: &[u8]) -> Option<PieceList> {
        let count = blob.count();
        if count <= 1 {
            return bytes.is_empty().then_some(PieceList {
                blob,
                cvs: Vec::new(),
            });
        }
        if bytes.len() != count as usize * CV_LEN {
            return None;
        }
        let (chunks, _) = bytes.as_chunks::<CV_LEN>();
        let cvs = chunks.to_vec();
        (root_of(&cvs, blob.size, blob.piece) == blob.root).then_some(PieceList { blob, cvs })
    }

    /// The blob this list belongs to.
    pub fn blob(&self) -> Blob {
        self.blob
    }

    /// The list as it travels and is stored: the chaining values one after
    /// the other, 32 bytes each. Empty for a blob of at most one piece.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.cvs.concat()
    }

    /// Whether `piece` is the genuine bytes of piece `index`.
    ///
    /// `false` when `index` is past the last piece or `piece` is not exactly
    /// as long as that piece. The check binds the position: genuine bytes in
    /// another slot fail.
    pub fn check(&self, index: u32, piece: &[u8]) -> bool {
        let Some(span) = self.blob.span(index) else {
            return false;
        };
        if piece.len() as u64 != span.end - span.start {
            return false;
        }
        if self.blob.count() == 1 {
            return BlobId::of(piece) == self.blob.root;
        }
        self.cvs
            .get(index as usize)
            .is_some_and(|cv| *cv == chaining_value(index, piece, self.blob.piece))
    }
}

/// Hashes a blob as it is written and keeps the chaining value of every
/// piece, so one pass yields the root and the piece list.
pub struct TreeHasher {
    piece: PieceSize,
    /// The most pieces to keep; [`MAX_PIECES`] outside tests.
    cap: u32,
    cvs: Vec<ChainingValue>,
    current: blake3::Hasher,
    filled: u64,
    total: u64,
}

impl TreeHasher {
    /// A hasher that cuts the blob into pieces of `piece`.
    pub fn new(piece: PieceSize) -> TreeHasher {
        TreeHasher::with_cap(piece, MAX_PIECES)
    }

    fn with_cap(piece: PieceSize, cap: u32) -> TreeHasher {
        TreeHasher {
            piece,
            cap,
            cvs: Vec::new(),
            current: blake3::Hasher::new(),
            filled: 0,
            total: 0,
        }
    }

    /// The next bytes of the blob.
    ///
    /// Once the blob is past [`MAX_PIECES`] pieces the rest is ignored and
    /// [`TreeHasher::finish`] returns `None`, so memory stays bounded however
    /// much is written.
    pub fn update(&mut self, mut bytes: &[u8]) {
        let piece = self.piece.bytes();
        let limit = u64::from(self.cap) * piece;
        while !bytes.is_empty() && self.total <= limit {
            // A full piece is closed only once more bytes come: a blob that
            // ends on it has it as its last piece, or as its only one.
            if self.filled == piece {
                self.cvs.push(self.current.finalize_non_root());
                self.current = blake3::Hasher::new();
                self.current.set_input_offset(self.total);
                self.filled = 0;
            }
            let take = usize::try_from(piece - self.filled)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            self.current.update(&bytes[..take]);
            self.filled += take as u64;
            self.total += take as u64;
            bytes = &bytes[take..];
        }
    }

    /// The blob and its piece list; `None` when the blob has more than
    /// [`MAX_PIECES`] pieces.
    pub fn finish(self) -> Option<PieceList> {
        let piece = self.piece.bytes();
        if self.total.div_ceil(piece) > u64::from(self.cap) {
            return None;
        }
        let size = self.total;
        if self.cvs.is_empty() {
            // No bytes, or one piece: the whole blob is the one chunk run.
            let root = BlobId(*self.current.finalize().as_bytes());
            let blob = Blob::new(root, size, self.piece)?;
            return Some(PieceList {
                blob,
                cvs: Vec::new(),
            });
        }
        let mut cvs = self.cvs;
        cvs.push(self.current.finalize_non_root());
        let root = root_of(&cvs, size, self.piece);
        let blob = Blob::new(root, size, self.piece)?;
        Some(PieceList { blob, cvs })
    }
}

/// One bit per piece of a blob: which ones someone holds.
///
/// Bit `i` is bit `i % 8` (least significant first) of byte `i / 8`. The
/// padding bits of the last byte are always zero.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bits {
    len: u32,
    bytes: Vec<u8>,
}

impl Bits {
    /// `len` bits, all clear.
    pub fn new(len: u32) -> Bits {
        Bits {
            len,
            bytes: vec![0; (len as usize).div_ceil(8)],
        }
    }

    /// `len` bits, all set.
    pub fn full(len: u32) -> Bits {
        let mut bits = Bits {
            len,
            bytes: vec![0xff; (len as usize).div_ceil(8)],
        };
        if let Some(last) = bits.bytes.last_mut() {
            *last &= Bits::last_mask(len);
        }
        bits
    }

    /// The bits that [`Bits::as_bytes`] wrote. `None` when `bytes` is not
    /// exactly as long as `len` bits take, or a padding bit is set.
    pub fn from_bytes(len: u32, bytes: &[u8]) -> Option<Bits> {
        if bytes.len() != (len as usize).div_ceil(8) {
            return None;
        }
        if let Some(last) = bytes.last()
            && last & !Bits::last_mask(len) != 0
        {
            return None;
        }
        Some(Bits {
            len,
            bytes: bytes.to_vec(),
        })
    }

    /// The mask of the bits of the last byte that belong to a map of `len`
    /// bits.
    fn last_mask(len: u32) -> u8 {
        match len % 8 {
            0 => 0xff,
            used => (1u8 << used) - 1,
        }
    }

    /// The bits as they travel: `ceil(len / 8)` bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// How many bits there are.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Whether there are no bits at all.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether bit `index` is set; `false` past the end.
    pub fn get(&self, index: u32) -> bool {
        index < self.len
            && self
                .bytes
                .get((index / 8) as usize)
                .is_some_and(|byte| byte & (1 << (index % 8)) != 0)
    }

    /// Sets or clears bit `index`; does nothing past the end.
    pub fn set(&mut self, index: u32, on: bool) {
        if index >= self.len {
            return;
        }
        if let Some(byte) = self.bytes.get_mut((index / 8) as usize) {
            if on {
                *byte |= 1 << (index % 8);
            } else {
                *byte &= !(1 << (index % 8));
            }
        }
    }

    /// How many bits are set.
    pub fn ones(&self) -> u32 {
        self.bytes.iter().map(|byte| byte.count_ones()).sum()
    }

    /// Whether every bit is set; true for no bits at all.
    pub fn all(&self) -> bool {
        self.ones() == self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic bytes that do not repeat at any piece boundary.
    fn data(len: usize) -> Vec<u8> {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (state >> 33) as u8
            })
            .collect()
    }

    fn hashed(bytes: &[u8], piece: PieceSize) -> PieceList {
        let mut hasher = TreeHasher::new(piece);
        for write in bytes.chunks(7919) {
            hasher.update(write);
        }
        hasher.finish().expect("within MAX_PIECES")
    }

    fn sizes(p: usize) -> Vec<usize> {
        vec![
            0,
            16,
            p - 1,
            p,
            p + 1,
            2 * p - 1,
            2 * p,
            2 * p + 1,
            3 * p + 5,
            4 * p,
            5 * p + p / 2,
            8 * p + 3,
        ]
    }

    #[test]
    fn the_root_is_the_hash_of_the_whole_blob() {
        let p = PieceSize::MIN.bytes() as usize;
        for size in sizes(p) {
            let bytes = data(size);
            let list = hashed(&bytes, PieceSize::MIN);
            assert_eq!(list.blob().root(), BlobId::of(&bytes), "size {size}");
            assert_eq!(list.blob().size(), size as u64, "size {size}");
            assert_eq!(
                list.blob().count() as usize,
                size.div_ceil(p),
                "size {size}"
            );
            let expected_cvs = if size.div_ceil(p) >= 2 {
                size.div_ceil(p)
            } else {
                0
            };
            assert_eq!(list.to_bytes().len(), expected_cvs * 32, "size {size}");
            assert_eq!(
                PieceList::of(&bytes, PieceSize::MIN).as_ref(),
                Some(&list),
                "one-shot, size {size}"
            );
        }
        let bytes = data(3 * PieceSize::DEFAULT.bytes() as usize + 5);
        let list = hashed(&bytes, PieceSize::DEFAULT);
        assert_eq!(list.blob().root(), BlobId::of(&bytes));
        assert_eq!(list.blob().count(), 4);
    }

    #[test]
    fn the_write_boundaries_do_not_change_the_list() {
        let bytes = data(5 * 1024 + 512);
        let whole = PieceList::of(&bytes, PieceSize::MIN).unwrap();
        let mut hasher = TreeHasher::new(PieceSize::MIN);
        for byte in bytes.chunks(1) {
            hasher.update(byte);
        }
        assert_eq!(hasher.finish().unwrap(), whole);
    }

    #[test]
    fn a_list_parses_back_from_its_bytes() {
        let p = PieceSize::MIN.bytes() as usize;
        for size in sizes(p) {
            let list = hashed(&data(size), PieceSize::MIN);
            let parsed = PieceList::parse(list.blob(), &list.to_bytes());
            assert_eq!(parsed.as_ref(), Some(&list), "size {size}");
        }
    }

    #[test]
    fn a_list_that_does_not_hash_to_the_root_is_refused() {
        let p = PieceSize::MIN.bytes() as usize;
        let list = hashed(&data(5 * p + p / 2), PieceSize::MIN);
        let bytes = list.to_bytes();
        for at in [0, 31, 32, bytes.len() / 2, bytes.len() - 1] {
            let mut changed = bytes.clone();
            changed[at] ^= 0x01;
            assert!(
                PieceList::parse(list.blob(), &changed).is_none(),
                "byte {at}"
            );
        }
        // The values in another order merge to another root.
        let mut swapped = bytes.clone();
        let (first, rest) = swapped.split_at_mut(32);
        first.swap_with_slice(&mut rest[..32]);
        assert!(PieceList::parse(list.blob(), &swapped).is_none());
        // A list for another root.
        let other = Blob::new(BlobId::of(b"other"), list.blob().size(), PieceSize::MIN).unwrap();
        assert!(PieceList::parse(other, &bytes).is_none());
    }

    #[test]
    fn a_list_of_the_wrong_length_is_refused() {
        let p = PieceSize::MIN.bytes() as usize;
        let list = hashed(&data(5 * p), PieceSize::MIN);
        let bytes = list.to_bytes();
        assert!(PieceList::parse(list.blob(), &bytes[..bytes.len() - 32]).is_none());
        assert!(PieceList::parse(list.blob(), &bytes[..bytes.len() - 1]).is_none());
        let mut longer = bytes.clone();
        longer.extend_from_slice(&[0; 32]);
        assert!(PieceList::parse(list.blob(), &longer).is_none());
        assert!(PieceList::parse(list.blob(), &[]).is_none());

        // A blob of at most one piece has no list at all.
        for size in [0, 1, p] {
            let one = hashed(&data(size), PieceSize::MIN);
            assert!(PieceList::parse(one.blob(), &[]).is_some(), "size {size}");
            assert!(
                PieceList::parse(one.blob(), &[0; 32]).is_none(),
                "size {size}"
            );
        }
    }

    #[test]
    fn a_piece_is_checked_against_its_slot() {
        let p = PieceSize::MIN.bytes() as usize;
        let bytes = data(5 * p + p / 2);
        let list = hashed(&bytes, PieceSize::MIN);
        let count = list.blob().count();
        assert_eq!(count, 6);

        // Every genuine piece passes, the short last one included.
        for index in 0..count {
            let span = list.blob().span(index).unwrap();
            let piece = &bytes[span.start as usize..span.end as usize];
            assert!(list.check(index, piece), "piece {index}");

            // One flipped bit fails, in the first and in the last byte.
            for at in [0, piece.len() - 1] {
                let mut bad = piece.to_vec();
                bad[at] ^= 0x80;
                assert!(!list.check(index, &bad), "piece {index}, byte {at}");
            }
            // Short by one byte and one byte too long fail.
            assert!(
                !list.check(index, &piece[..piece.len() - 1]),
                "piece {index}"
            );
            let mut long = piece.to_vec();
            long.push(0);
            assert!(!list.check(index, &long), "piece {index}");
            assert!(!list.check(index, &[]), "piece {index}");
        }

        // Genuine bytes in another slot fail.
        assert!(!list.check(1, &bytes[..p]));
        assert!(!list.check(0, &bytes[p..2 * p]));
        // Past the last piece fails.
        assert!(!list.check(count, &bytes[..p]));
        assert!(!list.check(u32::MAX, &bytes[..p]));
    }

    #[test]
    fn equal_pieces_merge_to_the_root_and_are_checked_in_their_own_slot() {
        // Pieces with equal bytes differ only by their offset in the tree.
        let p = PieceSize::MIN.bytes() as usize;
        let bytes = vec![7u8; 3 * p];
        let list = hashed(&bytes, PieceSize::MIN);
        assert_eq!(list.blob().root(), BlobId::of(&bytes));
        for index in 0..3 {
            assert!(list.check(index, &bytes[..p]), "piece {index}");
        }
        // Another blob does not accept them where its own bytes differ.
        let mut mixed = bytes.clone();
        mixed[p] = 8;
        let mixed = hashed(&mixed, PieceSize::MIN);
        assert!(mixed.check(0, &bytes[..p]));
        assert!(!mixed.check(1, &bytes[..p]));
        assert!(mixed.check(2, &bytes[..p]));
    }

    #[test]
    fn a_blob_of_one_piece_is_checked_against_the_root() {
        let p = PieceSize::MIN.bytes() as usize;
        for size in [1, 500, p - 1, p] {
            let bytes = data(size);
            let list = hashed(&bytes, PieceSize::MIN);
            assert_eq!(list.blob().count(), 1);
            assert!(list.check(0, &bytes), "size {size}");
            let mut bad = bytes.clone();
            bad[size / 2] ^= 0x01;
            assert!(!list.check(0, &bad), "size {size}");
            assert!(!list.check(0, &bytes[..size - 1]), "size {size}");
            assert!(!list.check(1, &bytes), "size {size}");
        }
    }

    #[test]
    fn an_empty_blob_has_no_piece() {
        let list = hashed(&[], PieceSize::MIN);
        assert_eq!(list.blob().root(), BlobId::of(b""));
        assert_eq!(list.blob().count(), 0);
        assert!(list.blob().span(0).is_none());
        assert!(!list.check(0, &[]));
    }

    #[test]
    fn a_blob_that_ends_on_a_piece_boundary_keeps_its_last_piece() {
        let p = PieceSize::MIN.bytes() as usize;
        for pieces in [1, 2, 3, 4, 8] {
            let bytes = data(pieces * p);
            let list = hashed(&bytes, PieceSize::MIN);
            assert_eq!(list.blob().count() as usize, pieces);
            assert_eq!(list.blob().root(), BlobId::of(&bytes), "{pieces} pieces");
        }
    }

    #[test]
    fn a_hasher_stops_storing_past_the_cap() {
        let p = PieceSize::MIN.bytes() as usize;
        let bytes = data(10 * p);

        let mut at_cap = TreeHasher::with_cap(PieceSize::MIN, 4);
        at_cap.update(&bytes[..4 * p]);
        assert_eq!(at_cap.finish().unwrap().blob().count(), 4);

        let mut over = TreeHasher::with_cap(PieceSize::MIN, 4);
        over.update(&bytes[..4 * p + 1]);
        assert!(over.finish().is_none());

        let mut far_over = TreeHasher::with_cap(PieceSize::MIN, 4);
        for write in bytes.chunks(p) {
            far_over.update(write);
        }
        assert!(far_over.cvs.len() <= 4, "stored {}", far_over.cvs.len());
        assert!(far_over.finish().is_none());
    }

    #[test]
    fn piece_sizes_are_powers_of_two_from_1_kib_to_16_mib() {
        assert_eq!(PieceSize::MIN.bytes(), 1024);
        assert_eq!(PieceSize::MAX.bytes(), 16 << 20);
        assert_eq!(PieceSize::DEFAULT.bytes(), 1 << 20);
        assert_eq!(PieceSize::default(), PieceSize::DEFAULT);
        assert_eq!(PieceSize::DEFAULT.log2(), 20);

        assert_eq!(PieceSize::from_log2(9), None);
        assert_eq!(PieceSize::from_log2(10), Some(PieceSize::MIN));
        assert_eq!(PieceSize::from_log2(24), Some(PieceSize::MAX));
        assert_eq!(PieceSize::from_log2(25), None);
        assert_eq!(PieceSize::from_log2(255), None);

        assert_eq!(PieceSize::new(1024), Some(PieceSize::MIN));
        assert_eq!(PieceSize::new(1 << 20), Some(PieceSize::DEFAULT));
        assert_eq!(PieceSize::new(16 << 20), Some(PieceSize::MAX));
        assert_eq!(PieceSize::new(0), None);
        assert_eq!(PieceSize::new(512), None);
        assert_eq!(PieceSize::new(1000), None);
        assert_eq!(PieceSize::new(3 << 10), None);
        assert_eq!(PieceSize::new(32 << 20), None);
        assert_eq!(PieceSize::new(1 << 63), None);
    }

    #[test]
    fn a_blob_has_at_most_max_pieces_and_an_empty_one_has_the_empty_root() {
        let piece = PieceSize::MIN;
        let root = BlobId::of(b"x");
        let max = u64::from(MAX_PIECES) * piece.bytes();
        assert!(Blob::new(root, max, piece).is_some());
        assert_eq!(Blob::new(root, max, piece).unwrap().count(), MAX_PIECES);
        assert!(Blob::new(root, max + 1, piece).is_none());
        assert!(Blob::new(root, u64::MAX, piece).is_none());
        // The same size is fewer pieces at a larger piece size.
        assert!(Blob::new(root, max + 1, PieceSize::DEFAULT).is_some());

        assert!(Blob::new(root, 0, piece).is_none());
        assert!(Blob::new(BlobId::from_bytes([0; 32]), 0, piece).is_none());
        assert!(Blob::new(BlobId::of(b""), 0, piece).is_some());
    }

    #[test]
    fn a_span_is_a_whole_piece_and_the_last_one_is_short() {
        let blob = Blob::new(BlobId::of(b"x"), 2 * 1024 + 100, PieceSize::MIN).unwrap();
        assert_eq!(blob.count(), 3);
        assert_eq!(blob.span(0), Some(0..1024));
        assert_eq!(blob.span(1), Some(1024..2048));
        assert_eq!(blob.span(2), Some(2048..2148));
        assert_eq!(blob.span(3), None);

        let exact = Blob::new(BlobId::of(b"x"), 2 * 1024, PieceSize::MIN).unwrap();
        assert_eq!(exact.count(), 2);
        assert_eq!(exact.span(1), Some(1024..2048));
        assert_eq!(exact.span(2), None);
    }

    #[test]
    fn bits_have_their_padding_clear() {
        for (len, bytes) in [(10u32, 2usize), (8, 1), (9, 2), (1, 1), (0, 0)] {
            let bits = Bits::full(len);
            assert_eq!(bits.len(), len);
            assert_eq!(bits.as_bytes().len(), bytes);
            assert_eq!(bits.ones(), len, "len {len}");
            assert!(bits.all());
            assert_eq!(Bits::from_bytes(len, bits.as_bytes()), Some(bits));
        }
        assert_eq!(Bits::full(10).as_bytes(), &[0xff, 0b11]);

        // A set padding bit is refused, in any padding position.
        assert!(Bits::from_bytes(10, &[0xff, 0b100]).is_none());
        assert!(Bits::from_bytes(10, &[0xff, 0x80]).is_none());
        assert!(Bits::from_bytes(10, &[0xff, 0b11]).is_some());
        assert!(Bits::from_bytes(1, &[0b10]).is_none());
        // A whole byte of bits has no padding.
        assert!(Bits::from_bytes(8, &[0xff]).is_some());
        assert!(Bits::from_bytes(0, &[]).is_some());
    }

    #[test]
    fn bits_of_the_wrong_length_are_refused() {
        assert!(Bits::from_bytes(10, &[0xff]).is_none());
        assert!(Bits::from_bytes(10, &[0, 0, 0]).is_none());
        assert!(Bits::from_bytes(8, &[]).is_none());
        assert!(Bits::from_bytes(0, &[0]).is_none());
        assert!(Bits::from_bytes(17, &[0, 0]).is_none());
        assert!(Bits::from_bytes(17, &[0, 0, 1]).is_some());
    }

    #[test]
    fn bits_set_get_and_count() {
        let mut bits = Bits::new(10);
        assert!(!bits.is_empty());
        assert!(Bits::new(0).is_empty());
        assert_eq!(bits.ones(), 0);
        assert!(!bits.all());

        bits.set(0, true);
        bits.set(7, true);
        bits.set(8, true);
        bits.set(9, true);
        assert_eq!(bits.as_bytes(), &[0b1000_0001, 0b11]);
        assert_eq!(bits.ones(), 4);
        assert!(bits.get(0) && bits.get(7) && bits.get(8) && bits.get(9));
        assert!(!bits.get(1) && !bits.get(6));

        // Past the end reads clear and writes nothing, so padding stays clear.
        assert!(!bits.get(10));
        assert!(!bits.get(15));
        assert!(!bits.get(u32::MAX));
        bits.set(10, true);
        bits.set(15, true);
        bits.set(u32::MAX, true);
        assert_eq!(bits.as_bytes(), &[0b1000_0001, 0b11]);

        bits.set(7, false);
        assert!(!bits.get(7));
        assert_eq!(bits.ones(), 3);

        for index in 0..10 {
            bits.set(index, true);
        }
        assert!(bits.all());
        assert_eq!(bits, Bits::full(10));
        // An empty map is complete.
        assert!(Bits::new(0).all());
    }

    #[test]
    fn a_blob_id_is_64_hex_digits() {
        let empty = BlobId::of(b"");
        let hex = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
        assert_eq!(empty.to_string(), hex);
        assert_eq!(format!("{empty:?}"), format!("BlobId({hex})"));
        assert_eq!(hex.parse::<BlobId>(), Ok(empty));
        assert_eq!(hex.to_uppercase().parse::<BlobId>(), Ok(empty));

        let id = BlobId::of(&data(1000));
        assert_eq!(id.to_string().parse::<BlobId>(), Ok(id));
        assert_eq!(BlobId::from_bytes(*id.as_bytes()), id);
        assert_eq!(
            BlobId::from_bytes([0x0a; 32]).to_string(),
            "0a".repeat(32),
            "a leading zero nibble is written"
        );
    }

    #[test]
    fn a_blob_id_of_another_length_or_alphabet_is_refused() {
        let hex = BlobId::of(b"x").to_string();
        assert_eq!(hex.len(), 64);
        assert_eq!(hex[..63].parse::<BlobId>(), Err(ParseBlobIdError));
        assert_eq!(format!("{hex}0").parse::<BlobId>(), Err(ParseBlobIdError));
        assert_eq!("".parse::<BlobId>(), Err(ParseBlobIdError));
        assert_eq!(
            format!("g{}", &hex[1..]).parse::<BlobId>(),
            Err(ParseBlobIdError)
        );
        // A sign is not a digit.
        assert_eq!(
            format!("+{}", &hex[1..]).parse::<BlobId>(),
            Err(ParseBlobIdError)
        );
        assert_eq!(
            format!(" {}", &hex[1..]).parse::<BlobId>(),
            Err(ParseBlobIdError)
        );
        // 64 bytes, but not 64 digits.
        assert_eq!(
            format!("é{}", &hex[2..]).parse::<BlobId>(),
            Err(ParseBlobIdError)
        );
        assert_eq!(ParseBlobIdError.to_string(), "a blob id is 64 hex digits");
    }
}
