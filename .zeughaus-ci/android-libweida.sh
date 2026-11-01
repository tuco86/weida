#!/bin/sh
# /// ci
# on = ["tag v*", "nightly"]
# platform = "linux-amd64"
# image = "android"
# timeout_minutes = 60
# ///
# libweida cross-built for the four Android ABIs with the NDK
# (docs/decisions/0046), each library checked for its ELF machine, and handed
# on as $CI_OUTPUT/jniLibs/<abi>/libweida.so. API 24 (Android 7.0) is the
# floor this build uses. It builds from source, so it needs no c-abi job, and
# it stays off the push pipeline to keep pushes short.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env

step cargo ndk --platform 24 -t arm64-v8a -t armeabi-v7a -t x86_64 -t x86 \
    -o "$CI_OUTPUT/jniLibs" build --release --locked -p weida-c

for pair in arm64-v8a:AArch64 armeabi-v7a:ARM x86_64:X86-64 x86:80386; do
    abi=${pair%%:*} machine=${pair#*:}
    lib="$CI_OUTPUT/jniLibs/$abi/libweida.so"
    step sh -c "echo '$lib'; readelf -h '$lib' | grep -q 'Machine:.*$machine'"
done
