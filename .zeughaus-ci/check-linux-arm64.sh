#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "nightly"]
# platform = "linux-arm64"
# image = "trixie"
# timeout_minutes = 120
# ///
# clippy, the tests and the doctests on linux-arm64: the unit tier on a push
# or manual run, both tiers on the nightly and a release tag.
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
exclude=""

check_platform
