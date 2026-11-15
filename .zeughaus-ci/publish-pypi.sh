#!/bin/bash
# /// ci
# needs = ["publish", "python-linux-amd64", "python-linux-arm64", "python-darwin-arm64", "python-windows-amd64"]
# channels = ["release"]
# platform = "linux-amd64"
# image = "python"
# secrets = ["PYPI_TOKEN"]
# timeout_minutes = 20
# ///
# Publishes `weida` to PyPI for a `v*` tag, after crates.io (`publish`), with
# the wheels the four python jobs handed on: manylinux x86_64 and aarch64,
# macosx_11_0_arm64 and win_amd64. Wheels only: an sdist would make pip build
# libweida from source, which needs a Rust toolchain. A wheel whose file name
# PyPI already has is left out, so a rerun after a partial upload finishes the
# rest and a rerun after a full one uploads nothing. A manual run (`run_jobs`)
# checks the wheels and runs `uv publish --dry-run`.
set -euo pipefail
shopt -s nullglob

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
export UV_CACHE_DIR="$CI_WORKSPACE/.ci-cache/uv"

version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
# PEP 440 spelling, as pep440() of hatch_build.py and smoke.py writes it:
# 0.1.0-alpha.16 is 0.1.0a16, 0.1.0-alpha.16-dev is 0.1.0a16.dev0.
pep440=$(printf '%s\n' "$version" | sed -e 's/-dev$/.dev0/' -e 's/-alpha\./a/' -e 's/-beta\./b/' -e 's/-rc\./rc/')

dist="$CI_WORKSPACE/.ci-cache/pypi-dist"
rm -rf "$dist"
mkdir -p "$dist"

# collect <job> <platform tag pattern>: the one wheel <job> handed on, named
# for this version and that platform, copied into $dist.
collect() {
    local job=$1 tag=$2
    local found=("$CI_INPUTS/$job"/wheels/weida-*.whl)
    if [ "${#found[@]}" -ne 1 ]; then
        echo "publish-pypi: $job handed on ${#found[@]} wheels in $CI_INPUTS/$job/wheels, expected one"
        exit 1
    fi
    local name=${found[0]##*/}
    case $name in
        weida-"$pep440"-py3-none-$tag.whl) ;;
        *)
            echo "publish-pypi: $name from $job is not weida-$pep440-py3-none-$tag.whl"
            exit 1
            ;;
    esac
    cp "${found[0]}" "$dist/"
}
collect python-linux-amd64 'manylinux_2_*_x86_64'
collect python-linux-arm64 'manylinux_2_*_aarch64'
collect python-darwin-arm64 macosx_11_0_arm64
collect python-windows-amd64 win_amd64

step uvx --from twine==7.0.0 twine check --strict "$dist"/*.whl

# The file names PyPI has for this version; none when it has no such version
# or no project `weida` yet (404). Any other failure stops the job.
python=$(uv python find cpython-3.14.7)
present=$("$python" - "$pep440" <<'EOF'
import json
import sys
import urllib.error
import urllib.request

try:
    with urllib.request.urlopen(f"https://pypi.org/pypi/weida/{sys.argv[1]}/json", timeout=60) as response:
        for entry in json.load(response)["urls"]:
            print(entry["filename"])
except urllib.error.HTTPError as error:
    if error.code != 404:
        raise
EOF
)

if [ "$CI_EVENT" = manual ]; then
    echo "PyPI has $(grep -c . <<<"$present" || true) files of weida $pep440"
    # Without a token uv prompts for a username; a dry run uploads nothing.
    step env UV_PUBLISH_TOKEN=dry-run uv publish --dry-run --trusted-publishing never "$dist"/*.whl
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
: "${PYPI_TOKEN:?secret PYPI_TOKEN is not available to this event}"

upload=()
for wheel in "$dist"/*.whl; do
    if grep -qxF "${wheel##*/}" <<<"$present"; then
        echo "${wheel##*/} is already on PyPI"
    else
        upload+=("$wheel")
    fi
done
if [ "${#upload[@]}" -eq 0 ]; then
    echo "weida $pep440 is already on PyPI"
    exit 0
fi
# The token goes through the environment, so step's echo of the command
# never prints it.
export UV_PUBLISH_TOKEN="$PYPI_TOKEN"
step uv publish --trusted-publishing never "${upload[@]}"
