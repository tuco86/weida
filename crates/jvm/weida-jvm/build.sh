#!/bin/sh
# Builds weida for the JVM into build/: the JNI library
# build/native/libweida_jni.so (libweida_jni.dylib on macOS), compiled with
# the system C compiler against crates/weida-c/include/weida.h and the
# libweida in $WEIDA_LIB_DIR, which is copied beside it where the library's
# rpath finds it; then the Java API, compiled for Java 17 and packed into
# build/weida-jvm.jar with the workspace version as Implementation-Version.
#
# Usage: WEIDA_LIB_DIR=<dir with libweida.so or libweida.dylib> build.sh
# JAVA_HOME is taken from the environment, or derived from javac on PATH.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)

if [ -z "${WEIDA_LIB_DIR:-}" ]; then
    echo "build.sh: set WEIDA_LIB_DIR to the directory holding libweida.so or libweida.dylib" >&2
    exit 1
fi

if [ -z "${JAVA_HOME:-}" ]; then
    javac_path=$(command -v javac) || {
        echo "build.sh: set JAVA_HOME, or put javac on PATH" >&2
        exit 1
    }
    # readlink -f resolves the /usr/bin/javac symlink chain to the JDK.
    JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$javac_path")")")
fi

case $(uname -s) in
    Darwin)
        lib=libweida.dylib jni=libweida_jni.dylib os_include=darwin
        rpath="-Wl,-rpath,@loader_path"
        ;;
    *)
        lib=libweida.so jni=libweida_jni.so os_include=linux
        rpath="-Wl,-rpath,\$ORIGIN"
        ;;
esac

if [ ! -f "$WEIDA_LIB_DIR/$lib" ]; then
    echo "build.sh: $WEIDA_LIB_DIR/$lib does not exist" >&2
    exit 1
fi

# [workspace.package] version of the root Cargo.toml.
version=$(awk '
    /^\[/ { section = $0 }
    section == "[workspace.package]" && $1 == "version" { gsub(/"/, "", $3); print $3; exit }
' "$root/Cargo.toml")
if [ -z "$version" ]; then
    echo "build.sh: no [workspace.package] version in $root/Cargo.toml" >&2
    exit 1
fi

rm -rf "$dir/build/native" "$dir/build/classes"
mkdir -p "$dir/build/native" "$dir/build/classes"

cc -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared \
    -I "$JAVA_HOME/include" -I "$JAVA_HOME/include/$os_include" -I "$root/crates/weida-c/include" \
    "$dir/src/main/c/weida_jni.c" \
    -L "$WEIDA_LIB_DIR" -lweida "$rpath" \
    -o "$dir/build/native/$jni"
cp "$WEIDA_LIB_DIR/$lib" "$dir/build/native/$lib"

find "$dir/src/main/java" -name '*.java' >"$dir/build/sources.txt"
"$JAVA_HOME/bin/javac" --release 17 -Xlint:all -Werror -d "$dir/build/classes" @"$dir/build/sources.txt"

printf 'Implementation-Title: weida-jvm\nImplementation-Version: %s\n' "$version" >"$dir/build/manifest.txt"
rm -f "$dir/build/weida-jvm.jar"
"$JAVA_HOME/bin/jar" --create --file "$dir/build/weida-jvm.jar" --manifest "$dir/build/manifest.txt" \
    -C "$dir/build/classes" .
echo "built $dir/build/weida-jvm.jar ($version) and $dir/build/native/$jni against $WEIDA_LIB_DIR/$lib"
