"""The piece tree of SWARM (docs/c/swarm.md): a blob's root and pieces, its
piece list, the tree hasher and piece maps, handles both surfaces share, and
the fetch options."""

from typing import Optional, Tuple

from weida._errors import check
from weida._native import ffi, lib
from weida._values import _count, _Fields, _Handle, _Made, _millis, _of

_U32_MAX = 2**32 - 1


def _bounded(name: str, value, maximum: int) -> int:
    """`_count`, and at most `maximum`: the C parameter's range."""
    value = _count(name, value)
    if value > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return value


def _bytes_arg(value):
    """Any bytes-like as the `uint8_t` array C reads; cffi raises
    `TypeError` for anything else."""
    return ffi.from_buffer("uint8_t[]", value)


def blob_id_of(data) -> bytes:
    """The id of `data` (any bytes-like): its BLAKE3 hash, `BLOB_ID_LEN` bytes."""
    src = _bytes_arg(data)
    out = ffi.new("uint8_t[]", lib.WEIDA_BLOB_ID_LEN)
    check(lib.weida_blob_id_of(src, len(src), out))
    return ffi.buffer(out)[:]


class Blob(_Handle):
    """A blob of `size` bytes whose hash is `root`, cut into pieces of
    `2**piece_log2` bytes."""

    def __init__(self, root, size: int, piece_log2: int = lib.WEIDA_PIECE_SIZE_DEFAULT_LOG2) -> None:
        """`root` is any bytes-like of `BLOB_ID_LEN` bytes, else a
        `ValueError`. C refuses, with `InvalidArgument`, a `piece_log2`
        outside 10..24, more than `MAX_PIECES` pieces, or an empty blob whose
        root is not the hash of no bytes."""
        r = _bytes_arg(root)
        if len(r) != lib.WEIDA_BLOB_ID_LEN:
            raise ValueError(f"root is {lib.WEIDA_BLOB_ID_LEN} bytes")
        out = ffi.new("weida_blob **")
        check(
            lib.weida_blob_new(
                r,
                _bounded("size", size, 2**64 - 1),
                _bounded("piece_log2", piece_log2, 255),
                out,
            )
        )
        super().__init__(out[0], lib.weida_blob_drop)

    @classmethod
    def _wrap(cls, ptr) -> "Blob":
        blob = cls.__new__(cls)
        _Handle.__init__(blob, ptr, lib.weida_blob_drop)
        return blob

    def root(self) -> bytes:
        """The blob's id, its BLAKE3 hash: `BLOB_ID_LEN` bytes."""
        out = ffi.new("uint8_t[]", lib.WEIDA_BLOB_ID_LEN)
        check(lib.weida_blob_root(self._p(), out))
        return ffi.buffer(out)[:]

    def size(self) -> int:
        """The blob's length in bytes."""
        return lib.weida_blob_size(self._p())

    def piece_log2(self) -> int:
        """The piece size as a power of two."""
        return lib.weida_blob_piece_log2(self._p())

    def count(self) -> int:
        """How many pieces the blob has."""
        return lib.weida_blob_count(self._p())

    def span(self, index: int) -> Optional[Tuple[int, int]]:
        """The bytes piece `index` covers, `(start, end)` with `end`
        exclusive; `None` past the last piece."""
        start, end = ffi.new("uint64_t *"), ffi.new("uint64_t *")
        if check(lib.weida_blob_span(self._p(), _bounded("index", index, _U32_MAX), start, end)) == 1:
            return (start[0], end[0])
        return None


class PieceList(_Made):
    """A blob's piece list: one chaining value per piece, which checks each
    piece against the root on its own."""

    _DROP = lib.weida_piece_list_drop
    _MADE_BY = "PieceList.of, PieceList.parse, TreeHasher.finish or Fetch.list"

    @classmethod
    def of(cls, data, piece_log2: int = lib.WEIDA_PIECE_SIZE_DEFAULT_LOG2) -> "PieceList":
        """The piece list of `data` (any bytes-like) in one call."""
        src = _bytes_arg(data)
        out = ffi.new("weida_piece_list **")
        check(lib.weida_piece_list_of(src, len(src), _bounded("piece_log2", piece_log2, 255), out))
        return cls._wrap(out[0])

    @classmethod
    def parse(cls, blob: Blob, data) -> "PieceList":
        """The piece list of `blob` from its bytes (`to_bytes()`); raises
        `InvalidArgument` unless they are one chaining value per piece
        hashing to the root."""
        src = _bytes_arg(data)
        out = ffi.new("weida_piece_list **")
        check(lib.weida_piece_list_parse(_of("blob", blob, Blob), src, len(src), out))
        return cls._wrap(out[0])

    def blob(self) -> Blob:
        """The blob this list belongs to."""
        out = ffi.new("weida_blob **")
        check(lib.weida_piece_list_blob(self._p(), out))
        return Blob._wrap(out[0])

    def to_bytes(self) -> bytes:
        """The list's bytes, a copy; empty for a blob of at most one piece."""
        length = ffi.new("size_t *")
        ptr = lib.weida_piece_list_to_bytes(self._p(), length)
        if length[0] == 0:
            return b""
        return ffi.buffer(ptr, length[0])[:]

    def check(self, index: int, piece) -> bool:
        """Whether `piece` (any bytes-like) is piece `index` of the blob."""
        src = _bytes_arg(piece)
        return check(lib.weida_piece_list_check(self._p(), _bounded("index", index, _U32_MAX), src, len(src))) == 1


class TreeHasher(_Handle):
    """Hashes a blob fed in parts into its `PieceList`."""

    def __init__(self, piece_log2: int = lib.WEIDA_PIECE_SIZE_DEFAULT_LOG2) -> None:
        """A `piece_log2` outside 10..24 raises `InvalidArgument`."""
        out = ffi.new("weida_tree_hasher **")
        check(lib.weida_tree_hasher_new(_bounded("piece_log2", piece_log2, 255), out))
        super().__init__(out[0], lib.weida_tree_hasher_drop)

    def update(self, data) -> "TreeHasher":
        """Feeds the next bytes (any bytes-like); returns this hasher."""
        src = _bytes_arg(data)
        check(lib.weida_tree_hasher_update(self._p(), src, len(src)))
        return self

    def finish(self) -> PieceList:
        """The piece list of every byte fed. Consumes the hasher, also on
        failure: more than `MAX_PIECES` pieces is `InvalidArgument`."""
        out = ffi.new("weida_piece_list **")
        check(lib.weida_tree_hasher_finish(self._consume(), out))
        return PieceList._wrap(out[0])


class Bits(_Handle):
    """A piece map: one bit per piece, all clear when made with `Bits(length)`."""

    def __init__(self, length: int) -> None:
        out = ffi.new("weida_bits **")
        check(lib.weida_bits_new(_bounded("length", length, _U32_MAX), out))
        super().__init__(out[0], lib.weida_bits_drop)

    @classmethod
    def _wrap(cls, ptr) -> "Bits":
        bits = cls.__new__(cls)
        _Handle.__init__(bits, ptr, lib.weida_bits_drop)
        return bits

    @classmethod
    def full(cls, length: int) -> "Bits":
        """A map of `length` bits, all set."""
        out = ffi.new("weida_bits **")
        check(lib.weida_bits_full(_bounded("length", length, _U32_MAX), out))
        return cls._wrap(out[0])

    @classmethod
    def from_bytes(cls, length: int, data) -> "Bits":
        """A map of `length` bits from `as_bytes()`'s form; `InvalidArgument`
        unless `data` is `(length + 7) // 8` bytes with clear padding bits."""
        n = _bounded("length", length, _U32_MAX)
        src = _bytes_arg(data)
        out = ffi.new("weida_bits **")
        check(lib.weida_bits_from_bytes(n, src, len(src), out))
        return cls._wrap(out[0])

    def as_bytes(self) -> bytes:
        """The map's bytes, a copy: bit `i` is bit `i % 8`, least significant
        first, of byte `i // 8`."""
        p = self._p()
        n = (lib.weida_bits_len(p) + 7) // 8
        buf = ffi.new("uint8_t[]", max(n, 1))
        out_n = ffi.new("size_t *")
        check(lib.weida_bits_as_bytes(p, buf, n, out_n))
        return ffi.buffer(buf, out_n[0])[:]

    def len(self) -> int:
        """How many bits the map has."""
        return lib.weida_bits_len(self._p())

    def ones(self) -> int:
        """How many bits are set."""
        return lib.weida_bits_ones(self._p())

    def get(self, index: int) -> bool:
        """Whether bit `index` is set; `False` past the end."""
        return lib.weida_bits_get(self._p(), _bounded("index", index, _U32_MAX)) == 1

    def all(self) -> bool:
        """Whether every bit is set."""
        return lib.weida_bits_all(self._p()) == 1

    def set(self, index: int, on: bool) -> None:
        """Sets or clears bit `index`; past the end it does nothing."""
        check(lib.weida_bits_set(self._p(), _bounded("index", index, _U32_MAX), 1 if on else 0))


class _FetchOptions(_Fields):
    """`FetchOptions`, the keyword arguments of `Swarm.fetch`: `list` a
    `PieceList`, `stall` and `have_refresh` seconds, the rest `int`s."""

    _OUT = "weida_fetch_options **"
    _NEW = lib.weida_fetch_options_new
    _DROP = lib.weida_fetch_options_drop
    _FIELDS = (
        ("stall", lib.weida_fetch_options_set_stall_ms, _millis),
        ("min_rate", lib.weida_fetch_options_set_min_rate, _count),
        ("per_peer", lib.weida_fetch_options_set_per_peer, _count),
        ("max_inflight", lib.weida_fetch_options_set_max_inflight, _count),
        ("max_peers", lib.weida_fetch_options_set_max_peers, _count),
        ("have_refresh", lib.weida_fetch_options_set_have_refresh_ms, _millis),
    )

    def _known(self) -> set:
        return super()._known() | {"list"}

    def _extra(self, handle) -> None:
        if "list" in self._fields:
            check(lib.weida_fetch_options_set_list(handle, _of("list", self._fields["list"], PieceList)))


__all__ = ["Bits", "Blob", "PieceList", "TreeHasher", "blob_id_of"]
