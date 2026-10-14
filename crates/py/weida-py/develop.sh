#!/bin/sh
# Builds the Python extension from this checkout and runs its tests; see
# crates/py/develop.sh.
#
# The tests need no external process at all: both halves of every exchange
# are this library, which is what makes weida's own binding the easiest of the
# six to test.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
exec "$here/../develop.sh" "$here" "$@"
