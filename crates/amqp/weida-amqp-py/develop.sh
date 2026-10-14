#!/bin/sh
# Builds the Python extension from this checkout and runs its tests; see
# crates/py/develop.sh.
#
# Between `maturin develop` and pytest it builds the `fe2o3-amqp` peer the
# interop tests need - a `cargo` example of the library, because a foreign peer
# is what these tests must run against and a script of our own is not one.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
WEIDA_PY_EXTRA=pytest-asyncio
WEIDA_PY_PREPARE='cargo build -p weida-amqp --example fe2o3_peer'
export WEIDA_PY_EXTRA WEIDA_PY_PREPARE
exec "$here/../../py/develop.sh" "$here" "$@"
