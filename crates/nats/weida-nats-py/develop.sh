#!/bin/sh
# Builds the Python extension from this checkout and runs its tests; see
# crates/py/develop.sh.
#
# The tests need no `nats-server`: they script the server half in Python, as
# `crates/nats/weida-nats/tests/support/mod.rs` scripts it in Rust. The ones
# that do need the real binary skip with the install command in the message.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
exec "$here/../../py/develop.sh" "$here" "$@"
