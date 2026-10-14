#!/bin/sh
# Builds the Python extension from this checkout and runs its tests; see
# crates/py/develop.sh.
#
# The interop peer is the NNG C library, through `pynng`, which bundles and
# builds it. Absent, every interop test skips with this command in its skip
# message.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
WEIDA_PY_EXTRA=pynng
export WEIDA_PY_EXTRA
exec "$here/../../py/develop.sh" "$here" "$@"
