#!/bin/sh
# /// ci
# on = ["tag v*", "nightly"]
# platform = "linux-amd64"
# image = "android"
# timeout_minutes = 60
# ///
# libweida cross-built for the four Android ABIs with the NDK
# (docs/decisions/0046), and beside each one the JNI library of weida for the
# JVM (crates/jvm/weida-jvm, docs/libraries/weida-jvm.md) compiled with the
# NDK's clang against it; each library checked for its ELF machine, the JNI
# one also for its NEEDED libweida.so, and handed on as
# $CI_OUTPUT/jniLibs/<abi>/{libweida,libweida_jni}.so. Then the Java sources
# of weida for the JVM are compiled against Android API 24's android.jar,
# which fails on any class or method API 24 lacks. API 24 (Android 7.0) is
# the floor this build uses. It builds from source, so it needs no c-abi job,
# and it stays off the push pipeline to keep pushes short.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env

step cargo ndk --platform 24 -t arm64-v8a -t armeabi-v7a -t x86_64 -t x86 \
    -o "$CI_OUTPUT/jniLibs" build --release --locked -p weida-c

# jni.h comes from the NDK's sysroot, no JDK include directory.
ndk_bin="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/linux-x86_64/bin"
for row in arm64-v8a:aarch64-linux-android:AArch64 armeabi-v7a:armv7a-linux-androideabi:ARM \
    x86_64:x86_64-linux-android:X86-64 x86:i686-linux-android:80386; do
    abi=${row%%:*} rest=${row#*:}
    triple=${rest%%:*} machine=${rest#*:}
    dir="$CI_OUTPUT/jniLibs/$abi"
    step "$ndk_bin/${triple}24-clang" -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared \
        -I crates/weida-c/include crates/jvm/weida-jvm/src/main/c/weida_jni.c \
        -L "$dir" -lweida -o "$dir/libweida_jni.so"
    for lib in "$dir/libweida.so" "$dir/libweida_jni.so"; do
        step sh -c "echo '$lib'; readelf -h '$lib' | grep -q 'Machine:.*$machine'"
    done
    step sh -c "readelf -d '$dir/libweida_jni.so' | grep -q 'NEEDED.*\[libweida\.so\]'"
done

# Language level 8: javac compiles for 9 and up only against a modular
# platform, which android.jar is not.
step javac -source 8 -target 8 -Xlint:all,-options -Werror \
    -bootclasspath "$ANDROID_HOME/platforms/android-24/android.jar" \
    -d "$CI_WORKSPACE/.ci-cache/weida-jvm-android" \
    crates/jvm/weida-jvm/src/main/java/net/doodleshnookie/weida/*.java
