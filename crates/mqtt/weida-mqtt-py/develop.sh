#!/bin/sh
# Builds the Python extension from this checkout and runs its tests; see
# crates/py/develop.sh.
#
# The interop tests need a broker and skip with the command in their skip
# message when none answers. To run them:
#
#   cargo install rumqttd --version 0.20.0
#   rumqttd -c ../weida-mqtt/tests/interop/rumqttd.toml -q
set -eu

here=$(cd "$(dirname "$0")" && pwd)
WEIDA_PY_EXTRA=pytest-asyncio
export WEIDA_PY_EXTRA
exec "$here/../../py/develop.sh" "$here" "$@"
