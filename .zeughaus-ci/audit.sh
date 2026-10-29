#!/bin/sh
# /// ci
# on = ["cron 0 4 * * *", "tag v*"]
# platform = "linux-amd64"
# image = "trixie"
# timeout_minutes = 15
# ///
# The dependency check of docs/SECURITY.md: every crate of Cargo.lock against
# a fresh copy of the RustSec advisory database, and crates.io as the only
# source. Nightly and release only, so a new advisory never turns an
# unrelated push red; publish needs this job, so no release ships with an
# open advisory.
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env

# cargo-deny from its prebuilt release, checked against the release's
# published SHA-256, into the workspace's cargo home.
deny=cargo-deny-0.20.2-x86_64-unknown-linux-musl
if [ ! -x "$CARGO_HOME/bin/cargo-deny" ]; then
    mkdir -p "$CARGO_HOME/bin"
    curl -LsSf -o "/tmp/$deny.tar.gz" \
        "https://github.com/EmbarkStudios/cargo-deny/releases/download/0.20.2/$deny.tar.gz"
    echo "9f12ed4c49936e09b48bf862b595cde2fe64fcbd9d74dfacac6131ca824c8d5f  /tmp/$deny.tar.gz" \
        | sha256sum -c -
    tar xzf "/tmp/$deny.tar.gz" -C "$CARGO_HOME/bin" --strip-components=1 "$deny/cargo-deny"
fi

step cargo deny --locked check advisories sources
