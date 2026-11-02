#!/bin/sh
# /// ci
# needs = ["c-abi-linux-amd64"]
# platform = "linux-amd64"
# image = "jvm"
# timeout_minutes = 30
# ///
# weida for the JVM (docs/decisions/0046, docs/libraries/weida-jvm.md): the
# image's C compiler builds the C smoke program and the JNI library against
# the libweida c-abi-linux-amd64 handed on, javac the Java API; the unit
# tests run under JUnit. The nightly and a release tag also run the tests
# tagged io (QUIC on localhost) and package.sh (the jar and the native
# libraries alone in a fresh directory, one QUIC round trip).
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

step java -version
step cc --version
smoke_libweida

cd crates/jvm/weida-jvm
step env WEIDA_LIB_DIR="$(libweida_input)/lib" ./build.sh
case $(tier) in
    ci)
        step ./test.sh all
        step ./package.sh
        ;;
    *) step ./test.sh unit ;;
esac
