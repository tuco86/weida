#!/bin/sh
# Builds the release wheel and proves it works with no Rust toolchain present;
# see crates/py/package.sh.
#
# The fresh virtualenv installs no broker and no C library: what an AMQP 1.0
# client built on Proton cannot claim is exactly what this checks. It runs one
# accepted transfer against the tests' scripted peer.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
exec "$here/../../py/package.sh" "$here" weida_amqp
