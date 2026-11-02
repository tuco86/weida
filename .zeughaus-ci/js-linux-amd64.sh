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
# runtimes, and package.sh (the packed tarball in a fresh project).
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

step node --version
step bun --version
step deno --version
step cc --version
smoke_libweida

cd crates/js/weida-js
step env WEIDA_LIB_DIR="$(libweida_input)/lib" ./build.sh
case $(tier) in
    ci) tests=test transports="inproc quic" ;;
    *) tests=test/unit transports=inproc ;;
esac
step node --test "$tests"
for transport in $transports; do
    step node smoke.mjs "$transport"
    step bun smoke.mjs "$transport"
    step deno run --allow-read --allow-env --allow-ffi smoke.mjs "$transport"
done
if [ "$(tier)" = ci ]; then
    step ./package.sh
fi
