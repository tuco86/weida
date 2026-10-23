#!/bin/sh
# /// ci
# on = ["tag perf-*"]
# machine = "sadala"
# timeout_minutes = 60
# [env]
# WEIDA_PERF_SECONDS = "1800"
# ///
# The server side of the weida-perf internet chain on sadala (crates/perf):
# two SWARM seeders and the TCP and raw quinn baselines on the ports the
# Hetzner firewall opens for experiments (2334-2336, UDP and TCP), serving
# for WEIDA_PERF_SECONDS, then exiting. The client runs elsewhere with the
# same --ports. Manual runs only (run_jobs); a `perf-*` tag also starts it.
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env

step cargo build --locked --release -p weida-perf
"$CARGO_TARGET_DIR/release/weida-perf" serve \
    --ports main=2334,swarm=2335,quinn=2336,tcp=2336 \
    --for "$WEIDA_PERF_SECONDS"
