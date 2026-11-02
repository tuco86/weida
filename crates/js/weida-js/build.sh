#!/bin/sh
# Compiles the Node-API addon build/weida.node with the system C compiler
# against crates/weida-c/include/weida.h and the libweida in
# $WEIDA_LIB_DIR, and copies that library beside it, where the addon's
# rpath finds it.
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

case $(uname -s) in
    Darwin)
        lib=libweida.dylib
        link="-undefined dynamic_lookup -Wl,-rpath,@loader_path"
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

mkdir -p "$dir/build"
# shellcheck disable=SC2086 # $link is two words on purpose
cc -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared \
    -I "$node_include" -I "$root/crates/weida-c/include" \
    "$dir/src/addon.c" "$dir/src/waiter.c" \
    -L "$WEIDA_LIB_DIR" -lweida -lpthread $link \
    -o "$dir/build/weida.node"
cp "$WEIDA_LIB_DIR/$lib" "$dir/build/$lib"
echo "built $dir/build/weida.node against $WEIDA_LIB_DIR/$lib"
