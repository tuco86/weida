#!/bin/sh
# Builds a release wheel and proves it works with no Rust toolchain present.
#
# Usage: package.sh <binding-dir> <wheel-prefix>
#
# Every binding's own `package.sh` is a wrapper that calls this one with its
# directory and the file-name prefix of its wheel (`weida`, `weida_zmq`, ...).
# Three things, in one command:
#
#   1. `maturin build --release` produces an `abi3` wheel - one file for every
#      CPython from 3.9 on - carrying the metadata `pyproject.toml` declares
#      and the version `Cargo.toml` holds, which is the workspace version and
#      is not typed a second time anywhere.
#   2. A **fresh** virtualenv, in a temporary directory, installs that wheel
#      and nothing else.
#   3. That virtualenv runs the binding's `smoke.py` with `PATH` scrubbed of
#      `cargo`, `rustc` and `maturin`, so "the wheel needs no Rust toolchain"
#      is a fact this script establishes rather than an expectation.
#      `smoke.py` reads the workspace version from `WEIDA_WORKSPACE_VERSION`
#      and checks that the installed wheel carries it.
#
# The temporary virtualenv is removed on the way out; the wheel is left in
# `target/wheels/` for a human to look at.
set -eu

dir=${1:?usage: package.sh <binding-dir> <wheel-prefix>}
prefix=${2:?usage: package.sh <binding-dir> <wheel-prefix>}

root=$(cd "$(dirname "$0")/../.." && pwd)
venv="$root/.venv"

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

uv venv --allow-existing "$venv"
uv pip install --python "$venv" maturin

cd "$dir"
VIRTUAL_ENV="$venv" "$venv/bin/maturin" build --release --out "$CARGO_TARGET_DIR/wheels"

wheel=$(ls -1t "$CARGO_TARGET_DIR"/wheels/"$prefix"-*.whl | head -n 1)
echo "built $wheel"
case "$wheel" in
    *-abi3-*) : ;;
    *) echo "the wheel is not abi3: $wheel" >&2; exit 1 ;;
esac

fresh=$(mktemp -d)
trap 'rm -rf "$fresh"' EXIT
uv venv "$fresh/venv"
uv pip install --python "$fresh/venv" "$wheel"

WEIDA_WORKSPACE_VERSION=$("$venv/bin/python" -c 'import sys, tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["workspace"]["package"]["version"])' "$root/Cargo.toml")
export WEIDA_WORKSPACE_VERSION

# No toolchain: the wheel is a compiled extension and a Python program that
# installs it must not need a compiler to run it.
PATH=/usr/bin:/bin "$fresh/venv/bin/python" "$dir/smoke.py"
echo "the wheel runs with no Rust toolchain on PATH: $wheel"
