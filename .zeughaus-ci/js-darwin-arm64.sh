#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "nightly"]
# platform = "darwin-arm64"
# timeout_minutes = 45
# ///
# weida for JavaScript on atik's host (docs/libraries/weida-js.md). The host
# has no Node, Bun or Deno, so js_env downloads the js image's versions into
# the workspace cache. libweida is built here (cargo build --release -p
# weida-c), sharing the warm workspace with check-darwin-arm64. The rest is
# js_check, as on linux; the job hands prebuilds/darwin-arm64/ on to
# publish-npm.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env
js_env

step cargo build --release --locked -p weida-c
js_check "$CARGO_TARGET_DIR/release"
