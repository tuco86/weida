#!/bin/sh
# Compiles the NIF priv/weida_nif.so with the system C compiler against
# crates/weida-c/include/weida.h, erl_nif.h of the `erl` on PATH and the
# libweida in $WEIDA_LIB_DIR, and copies that library beside it, where the
# NIF's rpath finds it. rebar3 runs it before every compile
# (rebar.config's pre_hooks).
#
# Usage: WEIDA_LIB_DIR=<dir with libweida.so or libweida.dylib> c_src/build.sh
set -eu

dir=$(cd "$(dirname "$0")/.." && pwd)
root=$(cd "$dir/../../.." && pwd)

if [ -z "${WEIDA_LIB_DIR:-}" ]; then
    echo "build.sh: set WEIDA_LIB_DIR to the directory holding libweida.so or libweida.dylib" >&2
    exit 1
fi

erts_include=$(erl -noshell -eval \
    'io:format("~s/erts-~s/include", [code:root_dir(), erlang:system_info(version)]), halt().')

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

mkdir -p "$dir/priv"
# shellcheck disable=SC2086 # $link is two words on purpose
cc -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared \
    -I "$erts_include" -I "$root/crates/weida-c/include" \
    "$dir/c_src/weida_nif.c" \
    -L "$WEIDA_LIB_DIR" -lweida $link \
    -o "$dir/priv/weida_nif.so"
cp "$WEIDA_LIB_DIR/$lib" "$dir/priv/$lib"
