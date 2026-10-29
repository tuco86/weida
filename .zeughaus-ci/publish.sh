#!/bin/bash
# /// ci
# needs = ["check-linux-amd64", "check-linux-arm64", "check-windows-amd64", "check-darwin-arm64", "audit"]
# channels = ["release"]
# platform = "linux-amd64"
# image = "trixie"
# secrets = ["CARGO_REGISTRY_TOKEN"]
# when_busy = "freeze"
# timeout_minutes = 60
# ///
# Publishes the workspace to crates.io for a `v*` tag, once every platform
# checked it. A crate whose version crates.io already has is left
# out, so a rerun after a partial publish finishes the rest. A manual run
# (`run_jobs`) packages and verifies the workspace without uploading.
set -euo pipefail

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env

version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

if [ "$CI_EVENT" = manual ]; then
    # Sources packaged by an earlier run are stale (RELEASING.md in zeughaus,
    # step 4).
    rm -rf "$CARGO_HOME"/registry/src/-*/
    step cargo publish --locked --workspace --dry-run
    exit 0
fi
if [ "$CI_EVENT" != tag ]; then
    echo "publish runs for a v* tag only"
    exit 1
fi
if [ "$CI_REF" != "v$version" ]; then
    echo "tag $CI_REF does not match workspace version $version"
    exit 1
fi
case "$version" in
    *-dev)
        echo "a -dev version is never published"
        exit 1
        ;;
esac
: "${CARGO_REGISTRY_TOKEN:?secret CARGO_REGISTRY_TOKEN is not available to this event}"

# The sparse index answers 404 for a crate that was never published: a new
# crate, published with the rest. Every name here has at least four
# characters, so its index path is `<2>/<2>/<name>`.
excludes=()
packages=0
for n in $(cargo metadata --locked --no-deps --format-version 1 |
    jq -r '.packages[] | select(.publish != []) | .name'); do
    packages=$((packages + 1))
    if index=$(curl -fsS "https://index.crates.io/${n:0:2}/${n:2:2}/$n" 2>/dev/null) &&
        grep -q "\"vers\":\"$version\"" <<<"$index"; then
        echo "$n $version is already on crates.io"
        excludes+=(--exclude "$n")
    fi
done
if [ $((${#excludes[@]} / 2)) -eq "$packages" ]; then
    echo "$version is already on crates.io"
    exit 0
fi
# cargo orders the packages and reads the token from the environment.
step cargo publish --locked --workspace "${excludes[@]}"
