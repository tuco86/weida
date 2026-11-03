#!/bin/bash
# /// ci
# on = ["nightly"]
# platform = "linux-amd64"
# image = "fuzz"
# timeout_minutes = 120
# state = ["fuzz-corpus"]
# state_save = "always"
# ///
# Runs every cargo-fuzz target of the workspace's fuzz crates for a fixed
# time on the scheduled nightly (docs/SECURITY.md). A nightly queued on
# demand (`ci_run`, a rerun) skips the round and succeeds: it is there for
# the test tiers. A manual run gives each target a few seconds, which proves
# they build and start. Every target runs even after one failed; a crash
# input is printed as base64, because the job's output directory is not kept
# and the log is.
# The corpus lives in the job state store `fuzz-corpus`, one directory per
# target named `<crate path below crates/>/<target>` (`protocol/credit`,
# `nats/weida-nats-codec/op_stream`): the CI restores its newest version
# into $CI_STATE/fuzz-corpus before the job and saves the directory as a
# new version afterwards, after a failed round too, so each round starts
# from what the earlier ones found. After its run, every target's corpus is
# merged down to the inputs that add coverage, which keeps the store small.
# A skipped round removes the directory and saves nothing; a manual run
# starts from the stored corpus and saves nothing.
set -uo pipefail

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
if [ "$CI_EVENT" = nightly ] && ! scheduled_nightly; then
    echo "fuzz: skipped, the fuzz round runs on the scheduled nightly only (actor: $CI_ACTOR)"
    # No directory, no version: the versions the CI keeps stay real rounds.
    rm -rf "${CI_STATE:?}/fuzz-corpus"
    exit 0
fi
cargo_env
# Each fuzz crate is its own workspace and builds in its own fuzz/target,
# where cargo-fuzz looks for the binaries.
unset CARGO_TARGET_DIR

# Equals the toolchain fuzz.Containerfile installs; change both together.
toolchain=nightly-2026-10-28
seconds=60
[ "$CI_EVENT" = manual ] && seconds=5

failed=""
store="${CI_STATE:?}/fuzz-corpus"
for manifest in crates/*/fuzz/Cargo.toml crates/*/*/fuzz/Cargo.toml; do
    crate=${manifest%/fuzz/Cargo.toml}
    targets=$(cd "$crate" && cargo "+$toolchain" fuzz list) || {
        failed="$failed $crate:list"
        continue
    }
    for target in $targets; do
        corpus="$store/${crate#crates/}/$target"
        mkdir -p "$corpus"
        # A warm workspace keeps the ignored artifacts of an earlier run;
        # only this run's crash inputs are printed.
        rm -rf "$crate/fuzz/artifacts/$target"
        if ! (cd "$crate" && step cargo "+$toolchain" fuzz run "$target" "$corpus" -- \
                -max_total_time="$seconds" -max_len=20000); then
            failed="$failed $crate:$target"
            for f in "$crate/fuzz/artifacts/$target"/*; do
                [ -f "$f" ] && printf 'artifact %s base64 %s\n' "$f" "$(base64 -w0 "$f")"
            done
        fi
        # The merge `cargo fuzz cmin` runs, but with both directories in the
        # store: cmin renames between fuzz/ and the corpus, which fails
        # across the mounts of the workspace and the store.
        rm -rf "$corpus.min"
        mkdir "$corpus.min"
        if ! { (cd "$crate" && step cargo "+$toolchain" fuzz run "$target" "$corpus.min" "$corpus" -- \
                -merge=1) && rm -rf "$corpus" && mv "$corpus.min" "$corpus"; }; then
            failed="$failed $crate:$target:merge"
            rm -rf "$corpus.min"
        fi
    done
done

if [ -n "$failed" ]; then
    echo "fuzz FAILED:$failed"
    exit 1
fi
