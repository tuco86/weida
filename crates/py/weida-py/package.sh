#!/bin/sh
# Builds the release wheel and proves it works with no Rust toolchain present;
# see crates/py/package.sh.
#
# No broker, no server, no C library: weida is its own peer, which is what
# makes this the simplest of the six wheels to check. The fresh virtualenv runs
# a Req/Rep round trip over QUIC, a Push/Pull transfer, a Pub/Sub message and a
# **streamed fan-out of 10 MiB against an 8 MiB subscriber budget** - the
# payload `publish` refuses and `open` carries.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
exec "$here/../package.sh" "$here" weida
