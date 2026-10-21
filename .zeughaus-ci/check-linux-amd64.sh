#!/bin/sh
# /// ci
# needs = ["build-linux-amd64"]
# platform = "linux-amd64"
# image = "trixie"
# when_busy = "freeze"
# timeout_minutes = 120
# ///
# The gate of docs/LOOP.md §6 on linux-amd64: formatting, clippy, the tests
# (the unit tier on a push or manual run, both tiers on the nightly cron and
# a release tag), the doctests and the doc step.
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
exclude=""

step cargo fmt --all --check
check_platform
check_docs
