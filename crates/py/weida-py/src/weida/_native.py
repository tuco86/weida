"""The C library, loaded once: cffi in out-of-line ABI mode over libweida.

`_ffi.py` is generated at build time from `_cdef.h` (which cbindgen derives
from `crates/weida-c`), and the library file is copied beside it by the
build hook; neither is in the source tree.
"""

import os
import sys

from weida._ffi import ffi


def _library_name() -> str:
    if sys.platform == "win32":
        return "weida.dll"
    if sys.platform == "darwin":
        return "libweida.dylib"
    return "libweida.so"


lib = ffi.dlopen(os.path.join(os.path.dirname(os.path.abspath(__file__)), _library_name()))

__all__ = ["ffi", "lib"]
