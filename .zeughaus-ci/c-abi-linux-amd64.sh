#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "nightly"]
# platform = "linux-amd64"
# image = "trixie"
# timeout_minutes = 45
# ///
# The C ABI and the Python package on it (docs/decisions/0045): the committed
# headers match what cbindgen derives from crates/weida-c, and the Python
# tests run against an editable install (the unit tier on a push or manual
# run, both tiers on the nightly and a release tag, which also run the C
# smoke program and the wheel's smoke in a fresh virtualenv). Hands
# libweida.so, weida.h and the wheel on to the amd64 language jobs
# (build.sh, c_abi).
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
c_abi
