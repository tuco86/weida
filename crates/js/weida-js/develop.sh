#!/bin/sh
# Builds libweida and the addon against it, and runs the package's tests
# under Node.
#
# Usage: develop.sh [node --test args...]   (default: every test under test/)
#
# CARGO_TARGET_DIR is honoured, so a worktree that builds into its own
# target directory keeps doing so.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

(cd "$root" && cargo build --release --locked -p weida-c)
WEIDA_LIB_DIR="$CARGO_TARGET_DIR/release" "$dir/build.sh"

cd "$dir"
if [ $# -eq 0 ]; then
    set -- test
fi
node --test "$@"
