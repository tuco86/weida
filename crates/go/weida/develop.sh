#!/bin/sh
# Builds libweida and runs the Go tests against its static library.
#
# Usage: develop.sh [go test args...]   (default: ./..., the unit tier;
#        -tags integration ./... adds the QUIC tests)
#
# CARGO_TARGET_DIR is honoured, so a worktree that builds into its own
# target directory keeps doing so.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

# libweida.a is not one of weida-c's crate types (crates/weida-c/Cargo.toml).
(cd "$root" && cargo rustc --release --locked -p weida-c --lib --crate-type staticlib)

# A directory with the static library alone, so -lweida cannot pick
# libweida.so.
lib="$CARGO_TARGET_DIR/go-lib"
mkdir -p "$lib"
cp "$CARGO_TARGET_DIR/release/libweida.a" "$lib/"

cd "$dir"
if [ $# -eq 0 ]; then
    set -- ./...
fi
CGO_LDFLAGS="-L$lib" go test "$@"
