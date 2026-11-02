#!/bin/sh
# Compiles the JUnit tests against build/classes and runs them with the
# JUnit Platform Console Standalone jar in $JUNIT_CONSOLE_JAR, loading the
# JNI library build.sh built (-Xcheck:jni on).
#
# Usage: test.sh [unit|all]   unit (the default) leaves out the tests
#                              tagged io (QUIC on localhost)
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
tier=${1:-unit}

case $tier in
    unit) filter="--exclude-tag io" ;;
    all) filter= ;;
    *)
        echo "usage: test.sh [unit|all]" >&2
        exit 2
        ;;
esac

if [ -z "${JUNIT_CONSOLE_JAR:-}" ] || [ ! -f "$JUNIT_CONSOLE_JAR" ]; then
    echo "test.sh: set JUNIT_CONSOLE_JAR to junit-platform-console-standalone.jar" >&2
    exit 1
fi
if [ -z "${JAVA_HOME:-}" ]; then
    JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")
fi

jni=$(ls "$dir"/build/native/libweida_jni.* 2>/dev/null | head -n 1)
if [ ! -f "$dir/build/weida-jvm.jar" ] || [ -z "$jni" ]; then
    echo "test.sh: build/weida-jvm.jar or the JNI library is missing; run build.sh first" >&2
    exit 1
fi

rm -rf "$dir/build/test-classes"
mkdir -p "$dir/build/test-classes"
find "$dir/src/test/java" -name '*.java' >"$dir/build/test-sources.txt"
"$JAVA_HOME/bin/javac" --release 17 -Xlint:all -Werror \
    -cp "$dir/build/classes:$JUNIT_CONSOLE_JAR" -d "$dir/build/test-classes" @"$dir/build/test-sources.txt"

# shellcheck disable=SC2086 # $filter is two words or none on purpose
"$JAVA_HOME/bin/java" -Xcheck:jni --enable-native-access=ALL-UNNAMED -Dweida.jni.path="$jni" \
    -jar "$JUNIT_CONSOLE_JAR" execute --disable-banner --fail-if-no-tests \
    --class-path "$dir/build/classes:$dir/build/test-classes" \
    --scan-class-path "$dir/build/test-classes" $filter
