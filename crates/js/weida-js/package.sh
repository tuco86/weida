#!/bin/sh
# Packs the package and proves the tarball works with no Rust toolchain
# present: the workspace version goes into the packed package.json, the
# tarball installs offline into a fresh empty project, and smoke.mjs runs
# there over both transports under node, bun and deno, with PATH reduced to
# those three runtimes plus /usr/bin:/bin.
#
# Needs prebuilds/<target>/weida.node (build.sh) first; packs every prebuild
# present. The tarball stays in $CARGO_TARGET_DIR/npm.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
out="$CARGO_TARGET_DIR/npm"
mkdir -p "$out"

ls "$dir"/prebuilds/*/weida.node >/dev/null 2>&1 || {
    echo "package.sh: prebuilds/*/weida.node is missing; run build.sh first" >&2
    exit 1
}

# [workspace.package] version of the root Cargo.toml, already valid semver.
version=$(awk '
    /^\[/ { section = $0 }
    section == "[workspace.package]" && $1 == "version" { gsub(/"/, "", $3); print $3; exit }
' "$root/Cargo.toml")
if [ -z "$version" ]; then
    echo "package.sh: no [workspace.package] version in $root/Cargo.toml" >&2
    exit 1
fi

fresh=$(mktemp -d)
trap 'rm -rf "$fresh"' EXIT

mkdir "$fresh/package"
cp -R "$dir/package.json" "$dir/lib" "$dir/prebuilds" "$fresh/package/"
(cd "$fresh/package" && npm pkg set "version=$version")
tarball=$(cd "$fresh/package" && npm pack --silent --pack-destination "$out")
echo "packed $out/$tarball"

mkdir "$fresh/project"
cd "$fresh/project"
npm init -y >/dev/null
npm install --offline --no-audit --no-fund "$out/$tarball"
cp "$dir/smoke.mjs" .

installed=$(node -p 'JSON.parse(require("fs").readFileSync("node_modules/@tuco86/weida/package.json", "utf8")).version')
if [ "$installed" != "$version" ]; then
    echo "package.sh: the installed package is $installed, not the workspace version $version" >&2
    exit 1
fi

runtimes_path=$(for tool in node bun deno; do dirname "$(command -v "$tool")"; done | awk '!seen[$0]++' | paste -sd: -)
for transport in inproc quic; do
    PATH="$runtimes_path:/usr/bin:/bin" node smoke.mjs "$transport"
    PATH="$runtimes_path:/usr/bin:/bin" bun smoke.mjs "$transport"
    PATH="$runtimes_path:/usr/bin:/bin" deno run --allow-read --allow-env --allow-ffi smoke.mjs "$transport"
done
echo "weida $version runs from its tarball under node, bun and deno: $out/$tarball"
