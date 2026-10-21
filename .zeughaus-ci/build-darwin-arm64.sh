#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "cron 0 4 * * *"]
# platform = "darwin-arm64"
# timeout_minutes = 120
# ///
# Builds every test binary and example check-darwin-arm64 runs, so the check
# starts warm. No artifact: the check runs in the same workspace.
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
# atik's Python is the Xcode one, whose libpython the linker does not find;
# the bindings' test binaries link libpython.
exclude="--exclude weida-py-core --exclude weida-py --exclude weida-zmq-py --exclude weida-nng-py --exclude weida-mqtt-py --exclude weida-amqp-py --exclude weida-nats-py"

step cargo test --locked --workspace $exclude --no-run
