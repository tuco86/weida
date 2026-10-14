#!/bin/sh
# Builds the release wheel and proves it works with no Rust toolchain present;
# see crates/py/package.sh.
#
# The fresh virtualenv runs an asyncio REQ/REP round trip between two sockets
# of this module.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
exec "$here/../../py/package.sh" "$here" weida_zmq
