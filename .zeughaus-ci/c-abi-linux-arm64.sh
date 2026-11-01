#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "nightly"]
# platform = "linux-arm64"
# image = "trixie"
# timeout_minutes = 45
# ///
# The arm64 twin of c-abi-linux-amd64: the same header check and Python
# tests on linux-arm64, and the libweida.so, weida.h and wheel the arm64
# language jobs receive (build.sh, c_abi).
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
c_abi
