#!/bin/sh
# Proves that the jar and the native libraries work on their own: copies
# build/weida-jvm.jar and build/native/ into a fresh directory and runs
# smoke/Smoke.java there (one QUIC round trip) with PATH reduced to
# $JAVA_HOME/bin:/usr/bin:/bin, so no Rust toolchain is reachable.
#
# Needs build.sh first.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)

if [ -z "${JAVA_HOME:-}" ]; then
    JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")
fi

if [ ! -f "$dir/build/weida-jvm.jar" ] || [ ! -d "$dir/build/native" ]; then
    echo "package.sh: build/weida-jvm.jar or build/native/ is missing; run build.sh first" >&2
    exit 1
fi

fresh=$(mktemp -d)
trap 'rm -rf "$fresh"' EXIT

cp "$dir/build/weida-jvm.jar" "$fresh/"
cp -R "$dir/build/native" "$fresh/native"
mkdir "$fresh/smoke"
cp "$dir/smoke/Smoke.java" "$fresh/smoke/"

jni=$(ls "$fresh"/native/libweida_jni.* | head -n 1)
cd "$fresh"
PATH="$JAVA_HOME/bin:/usr/bin:/bin" java --enable-native-access=ALL-UNNAMED \
    -Dweida.jni.path="$jni" -cp weida-jvm.jar smoke/Smoke.java
