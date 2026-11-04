#!/bin/bash
# /// ci
# needs = ["publish", "js-linux-amd64", "js-linux-arm64", "js-darwin-arm64", "js-windows-amd64"]
# channels = ["release"]
# platform = "linux-amd64"
# image = "js"
# secrets = ["NPM_TOKEN"]
# timeout_minutes = 20
# ///
# Publishes `weida` to the public npm registry for a `v*` tag, after
# crates.io (`publish`) and with the prebuilds the four js jobs handed on.
# A version the registry already has is left alone, so a rerun is safe. A
# manual run (`run_jobs`) packs, smokes and runs `npm publish --dry-run`.
set -euo pipefail

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
export CARGO_TARGET_DIR="$CI_WORKSPACE/.ci-cache/target"
registry=https://registry.npmjs.org/
pkg=crates/js/weida-js

version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

rm -rf "$pkg/prebuilds"
for pair in linux-x64:js-linux-amd64 linux-arm64:js-linux-arm64 darwin-arm64:js-darwin-arm64 \
    win32-x64:js-windows-amd64; do
    target=${pair%%:*} job=${pair#*:}
    from="$CI_INPUTS/$job/prebuilds/$target"
    if [ ! -f "$from/weida.node" ]; then
        echo "publish-npm: $from/weida.node is missing; $job handed on no prebuild"
        exit 1
    fi
    mkdir -p "$pkg/prebuilds/$target"
    cp "$from"/* "$pkg/prebuilds/$target/"
done

# Packs all four prebuilds and smokes the tarball here, on linux-x64.
step "$pkg/package.sh"
tarball="$CARGO_TARGET_DIR/npm/weida-$version.tgz"

if [ "$CI_EVENT" = manual ]; then
    step npm publish --dry-run --tag latest --registry "$registry" "$tarball"
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
: "${NPM_TOKEN:?secret NPM_TOKEN is not available to this event}"

if npm view "weida@$version" version --registry "$registry" 2>/dev/null | grep -qx "$version"; then
    echo "weida $version is already in the registry"
    exit 0
fi

npmrc=$(mktemp)
trap 'rm -f "$npmrc"' EXIT
printf '//registry.npmjs.org/:_authToken=%s\n' "$NPM_TOKEN" >"$npmrc"
# Every version is a prerelease; `latest` lets `npm install weida` without a
# version find it.
step env NPM_CONFIG_USERCONFIG="$npmrc" npm publish --tag latest --registry "$registry" "$tarball"
