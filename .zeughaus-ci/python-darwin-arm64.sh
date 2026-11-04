#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "nightly"]
# platform = "darwin-arm64"
# timeout_minutes = 45
# ///
# weida's Python package on atik's host (docs/libraries/weida-py.md). uv_env
# downloads uv, whose CPython builds stand in for the Xcode Python. The wheel is
# built here: hatch_build.py runs cargo build --release -p weida-c in the warm
# workspace that check-darwin-arm64 and js-darwin-arm64 share, and tags the wheel
# with the macOS libweida is built for. python_check tests it on CPython 3.13
# and 3.14, free-threaded 3.14t and PyPy 3.11, and the job hands wheels/ on.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
uv_env

wheels="$CI_WORKSPACE/.ci-cache/py-wheels"
rm -rf "$wheels"
step uv build --wheel --python cpython-3.14.7 -o "$wheels" crates/py/weida-py
wheel=$(ls "$wheels"/weida-*.whl)
case $wheel in
    *-py3-none-macosx_11_0_arm64.whl) ;;
    *) echo "the wheel is not py3-none-macosx_11_0_arm64: $wheel" >&2; exit 1 ;;
esac
# The tag promises macOS 11, so the library must not ask for a newer one.
minos=$(otool -l "$CARGO_TARGET_DIR/release/libweida.dylib" | awk '$1 == "minos" { print $2; exit }')
if [ "$minos" != 11.0 ]; then
    echo "libweida.dylib needs macOS $minos, the wheel's tag says 11.0" >&2
    exit 1
fi

python_check "$wheel" cpython313:cpython-3.13.14 cpython:cpython-3.14.7 cpython-ft:cpython-3.14.7+freethreaded pypy:pypy-3.11.15
mkdir -p "$CI_OUTPUT/wheels"
cp "$wheel" "$CI_OUTPUT/wheels/"
