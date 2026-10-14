#!/bin/sh
# Builds the release wheel and proves it works with no Rust toolchain present;
# see crates/py/package.sh.
#
# The fresh virtualenv installs no `pynng` and no NNG C library: an SP
# implementation that needs neither at install time is the claim being
# checked. It runs one REQ/REP round trip on each surface, asyncio and
# synchronous. Both ends are this module, so nothing but the wheel is under
# test.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
exec "$here/../../py/package.sh" "$here" weida_nng
