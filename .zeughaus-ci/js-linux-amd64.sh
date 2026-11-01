#!/bin/sh
# /// ci
# needs = ["c-abi-linux-amd64"]
# platform = "linux-amd64"
# image = "js"
# timeout_minutes = 30
# ///
# The JavaScript environment (docs/decisions/0046): Node, Bun and Deno run,
# and the image's C compiler builds and runs the C smoke program against the
# libweida c-abi-linux-amd64 handed on.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

step node --version
step bun --version
step deno --version
step cc --version
smoke_libweida
