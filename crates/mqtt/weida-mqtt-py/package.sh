#!/bin/sh
# Builds the release wheel and proves it works with no Rust toolchain present;
# see crates/py/package.sh.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
exec "$here/../../py/package.sh" "$here" weida_mqtt
