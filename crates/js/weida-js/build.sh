#!/bin/sh
# Compiles the Node-API addon prebuilds/<platform>-<arch>/weida.node with the
# system C compiler against crates/weida-c/include/weida.h and the libweida
# in $WEIDA_LIB_DIR, and copies that library beside it, where the addon's
# rpath finds it. <platform>-<arch> is Node's process.platform-process.arch:
# linux-x64, linux-arm64 or darwin-arm64; build.ps1 builds win32-x64.
#
# Usage: WEIDA_LIB_DIR=<dir with libweida.so or libweida.dylib> build.sh
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)

if [ -z "${WEIDA_LIB_DIR:-}" ]; then
    echo "build.sh: set WEIDA_LIB_DIR to the directory holding libweida.so or libweida.dylib" >&2
    exit 1
fi

node_include=$(node -p "require('path').join(process.execPath, '..', '..', 'include', 'node')")
target=$(node -p 'process.platform + "-" + process.arch')
out="$dir/prebuilds/$target"

case $(uname -s) in
    Darwin)
        lib=libweida.dylib
        link="-undefined dynamic_lookup -Wl,-rpath,@loader_path"
        # The macOS SDK of the selected developer directory, as rustc picks it:
        # without it cc takes xcrun's default SDK, which can be a newer
        # CommandLineTools SDK whose .tbd files that toolchain's ld cannot read.
        if [ -z "${SDKROOT:-}" ]; then
            SDKROOT=$(xcrun --sdk macosx --show-sdk-path)
            export SDKROOT
        fi
        ;;
    *)
        lib=libweida.so
        link="-Wl,-rpath,\$ORIGIN"
        ;;
esac

if [ ! -f "$WEIDA_LIB_DIR/$lib" ]; then
    echo "build.sh: $WEIDA_LIB_DIR/$lib does not exist" >&2
    exit 1
fi

mkdir -p "$out"
# shellcheck disable=SC2086 # $link is two words on purpose
cc -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared \
    -I "$node_include" -I "$root/crates/weida-c/include" \
    "$dir/src/addon.c" "$dir/src/waiter.c" \
    -L "$WEIDA_LIB_DIR" -lweida -lpthread $link \
    -o "$out/weida.node"
cp "$WEIDA_LIB_DIR/$lib" "$out/$lib"
echo "built $out/weida.node against $WEIDA_LIB_DIR/$lib"
