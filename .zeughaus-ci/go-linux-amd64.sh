#!/bin/sh
# /// ci
# needs = ["c-abi-linux-amd64"]
# platform = "linux-amd64"
# image = "go"
# timeout_minutes = 30
# ///
# The Go environment (docs/decisions/0046): Go runs with cgo enabled, and the
# image's C compiler builds and runs the C smoke program against the libweida
# c-abi-linux-amd64 handed on.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

step go version
step sh -c 'cgo=$(go env CGO_ENABLED); echo "CGO_ENABLED=$cgo"; [ "$cgo" = 1 ]'
step cc --version
smoke_libweida
