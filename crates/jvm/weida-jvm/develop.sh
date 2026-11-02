#!/bin/sh
# Builds libweida, weida for the JVM against it, and runs the tests.
#
# Usage: develop.sh [unit|all]   (default: all)
#
# CARGO_TARGET_DIR is honoured, so a worktree that builds into its own
# target directory keeps doing so. The JUnit console jar is downloaded into
# $CARGO_TARGET_DIR/jvm/ when JUNIT_CONSOLE_JAR is unset and it is missing
# there: the version and sha256 .zeughaus-ci/jvm.Containerfile pins.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

junit_version=6.1.3
junit_sha256=e62b96ac475dbcde8599ea905d088f65d90778f86e259b856a49fa5c4ea256ec

if [ -z "${JUNIT_CONSOLE_JAR:-}" ]; then
    JUNIT_CONSOLE_JAR="$CARGO_TARGET_DIR/jvm/junit-platform-console-standalone-$junit_version.jar"
    if [ ! -f "$JUNIT_CONSOLE_JAR" ]; then
        mkdir -p "$CARGO_TARGET_DIR/jvm"
        curl -fsSLo "$JUNIT_CONSOLE_JAR.part" \
            "https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/$junit_version/junit-platform-console-standalone-$junit_version.jar"
        echo "$junit_sha256  $JUNIT_CONSOLE_JAR.part" | sha256sum -c -
        mv "$JUNIT_CONSOLE_JAR.part" "$JUNIT_CONSOLE_JAR"
    fi
fi
export JUNIT_CONSOLE_JAR

(cd "$root" && cargo build --release --locked -p weida-c)
WEIDA_LIB_DIR="$CARGO_TARGET_DIR/release" "$dir/build.sh"
"$dir/test.sh" "${1:-all}"
