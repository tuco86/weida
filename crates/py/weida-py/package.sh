#!/bin/sh
# Builds the wheel and proves it works with no Rust toolchain present: the
# wheel must be `py3-none-<platform>`, install into a fresh virtualenv, and
# run smoke.py there with PATH scrubbed of cargo, rustc and cc.
#
# With WEIDA_PY_PYTHON set (the CI image, which has no uv), that interpreter
# builds the wheel with pip and makes the virtualenv; otherwise uv does both.
# The wheel stays in $CARGO_TARGET_DIR/wheels.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR
wheels="$CARGO_TARGET_DIR/wheels"
mkdir -p "$wheels"

fresh=$(mktemp -d)
trap 'rm -rf "$fresh"' EXIT

if [ -n "${WEIDA_PY_PYTHON:-}" ]; then
    "$WEIDA_PY_PYTHON" -m pip wheel --no-build-isolation --no-deps -w "$wheels" "$dir"
    "$WEIDA_PY_PYTHON" -m venv "$fresh/venv"
    python="$WEIDA_PY_PYTHON"
else
    uv build --wheel -o "$wheels" "$dir"
    uv venv "$fresh/venv"
    python=python3
fi

wheel=$(ls -1t "$wheels"/weida-*.whl | head -n 1)
echo "built $wheel"
case "$wheel" in
    *-py3-none-*) : ;;
    *) echo "the wheel is not py3-none: $wheel" >&2; exit 1 ;;
esac

if [ -n "${WEIDA_PY_PYTHON:-}" ]; then
    "$fresh/venv/bin/pip" install "$wheel"
else
    uv pip install --python "$fresh/venv" "$wheel"
fi

WEIDA_WORKSPACE_VERSION=$("$python" -c 'import sys, tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["workspace"]["package"]["version"])' "$root/Cargo.toml")
export WEIDA_WORKSPACE_VERSION

PATH=/usr/bin:/bin "$fresh/venv/bin/python" "$dir/smoke.py"
echo "the wheel runs with no Rust toolchain on PATH: $wheel"
