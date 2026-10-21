#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "cron 0 4 * * *"]
# platform = "darwin-arm64"
# timeout_minutes = 120
# ///
# clippy, the tests and the doctests on darwin-arm64: the unit tier on a push
# or manual run, both tiers on the nightly cron and a release tag.
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
# atik's Python is the Xcode one, whose libpython the linker does not find;
# the bindings' test binaries link libpython.
exclude="--exclude weida-py-core --exclude weida-py --exclude weida-zmq-py --exclude weida-nng-py --exclude weida-mqtt-py --exclude weida-amqp-py --exclude weida-nats-py"

check_platform
