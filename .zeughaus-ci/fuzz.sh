#!/bin/bash
# /// ci
# on = ["cron 0 4 * * *"]
# platform = "linux-amd64"
# image = "fuzz"
# timeout_minutes = 120
# ///
# Runs every cargo-fuzz target of the workspace's fuzz crates for a fixed
# time on the nightly cron (docs/SECURITY.md). A manual run gives each target
# a few seconds, which proves they build and start. Every target runs even
# after one failed; a crash input is printed as base64, because the job's
# output directory is not kept and the log is.
set -uo pipefail

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
# Each fuzz crate is its own workspace and builds in its own fuzz/target,
# where cargo-fuzz looks for the binaries.
unset CARGO_TARGET_DIR

# Equals the toolchain fuzz.Containerfile installs; change both together.
toolchain=nightly-2026-10-28
seconds=60
[ "$CI_EVENT" = manual ] && seconds=5

failed=""
for manifest in crates/*/fuzz/Cargo.toml crates/*/*/fuzz/Cargo.toml; do
    crate=${manifest%/fuzz/Cargo.toml}
    targets=$(cd "$crate" && cargo "+$toolchain" fuzz list) || {
        failed="$failed $crate:list"
        continue
    }
    for target in $targets; do
        # A warm workspace keeps the ignored artifacts of an earlier run;
        # only this run's crash inputs are printed.
        rm -rf "$crate/fuzz/artifacts/$target"
        if ! (cd "$crate" && step cargo "+$toolchain" fuzz run "$target" -- \
                -max_total_time="$seconds" -max_len=20000); then
            failed="$failed $crate:$target"
            for f in "$crate/fuzz/artifacts/$target"/*; do
                [ -f "$f" ] && printf 'artifact %s base64 %s\n' "$f" "$(base64 -w0 "$f")"
            done
        fi
    done
done

if [ -n "$failed" ]; then
    echo "fuzz FAILED:$failed"
    exit 1
fi
