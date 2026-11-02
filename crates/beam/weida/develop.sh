#!/bin/sh
# Builds libweida and the NIF against it, and runs the EUnit tests.
#
# Usage: develop.sh [rebar3 eunit args...]   (default: both test modules;
#        --module=weida_tests runs the unit tier only)
#
# CARGO_TARGET_DIR is honoured, so a worktree that builds into its own
# target directory keeps doing so.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

(cd "$root" && cargo build --release --locked -p weida-c)

cd "$dir"
WEIDA_LIB_DIR="$CARGO_TARGET_DIR/release" rebar3 eunit "$@"
