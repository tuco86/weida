#!/bin/sh
# Regenerates the two headers cbindgen derives from this crate: include/weida.h
# for C callers and the cffi declarations of the Python package. With
# `--verify` as the first argument it writes nothing and fails when either is
# stale, which is what the CI checks.
set -eu

cd "$(dirname "$0")"

verify=""
if [ "${1:-}" = "--verify" ]; then
    verify="--verify"
fi

cbindgen $verify --config cbindgen.toml --crate weida-c --output include/weida.h
cbindgen $verify --config cbindgen-cffi.toml --crate weida-c --output ../py/weida-py/src/weida/_cdef.h
