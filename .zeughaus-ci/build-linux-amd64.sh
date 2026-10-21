#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "cron 0 4 * * *"]
# platform = "linux-amd64"
# image = "trixie"
# when_busy = "freeze"
# timeout_minutes = 120
# ///
# Builds every test binary and example check-linux-amd64 runs, so the check
# starts warm. No artifact: the check runs in the same workspace.
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
exclude=""

step cargo test --locked --workspace $exclude --no-run
