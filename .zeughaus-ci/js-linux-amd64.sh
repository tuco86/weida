#!/bin/sh
# /// ci
# needs = ["c-abi-linux-amd64"]
# platform = "linux-amd64"
# image = "js"
# timeout_minutes = 30
# ///
# weida for JavaScript (docs/decisions/0046, docs/libraries/weida-js.md): the
# image's C compiler builds the C smoke program and the Node-API addon
# against the libweida c-abi-linux-amd64 handed on; the unit tests run under
# Node and the in-process smoke under Node, Bun and Deno. The nightly and a
# release tag also run the integration tests, the QUIC smoke under all three
# runtimes, and package.sh (the packed tarball in a fresh project). The job
# hands prebuilds/<target>/ on to publish-npm.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

smoke_libweida
js_check "$(libweida_input)/lib"
