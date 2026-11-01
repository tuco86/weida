#!/bin/sh
# /// ci
# needs = ["c-abi-linux-arm64"]
# platform = "linux-arm64"
# image = "jvm"
# timeout_minutes = 30
# ///
# The JVM environment (docs/decisions/0046): the JDK runs, and the image's C
# compiler builds and runs the C smoke program against the libweida
# c-abi-linux-arm64 handed on.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

step java -version
step cc --version
smoke_libweida
