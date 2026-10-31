#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "nightly"]
# platform = "darwin-arm64"
# timeout_minutes = 120
# ///
# clippy, the tests and the doctests on darwin-arm64: the unit tier on a push
# or manual run, both tiers on the nightly and a release tag.
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
# atik's Python is the Xcode one, whose libpython the linker does not find;
# the bindings' test binaries link libpython.
exclude="--exclude weida-py-core --exclude weida-zmq-py --exclude weida-nng-py --exclude weida-mqtt-py --exclude weida-amqp-py --exclude weida-nats-py"

# macOS starts a process with a soft limit of 256 descriptors, below what a
# test holds with both ends of a local transport in one process
# (`max_local_streams` is 255 on each side); Linux starts at 1024 or more.
ulimit -n 10240 2>/dev/null || ulimit -n "$(ulimit -Hn)"

check_platform
