#!/bin/sh
# Builds libweida in release, compiles tests/c/smoke.c against include/weida.h
# with -Wall -Wextra -Werror, links it with -lweida and runs it: a QUIC
# Req/Rep through the C ABI that prints "c smoke: ok". Then compiles
# tests/c/easy.c against include/weida_easy.h the same way, with -pthread,
# and runs it: the same Req/Rep through the easy tier, printing "c easy: ok".
set -eu

root="$(cd "$(dirname "$0")/../.." && pwd)"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

cd "$root"
cargo build --release --locked -p weida-c
cc -std=c11 -Wall -Wextra -Werror -I crates/weida-c/include crates/weida-c/tests/c/smoke.c \
    -L "$CARGO_TARGET_DIR/release" -lweida -o "$CARGO_TARGET_DIR/weida-c-smoke"
cc -std=c11 -Wall -Wextra -Werror -pthread -I crates/weida-c/include crates/weida-c/tests/c/easy.c \
    -L "$CARGO_TARGET_DIR/release" -lweida -o "$CARGO_TARGET_DIR/weida-c-easy"

run() {
    case "$(uname -s)" in
    Darwin) DYLD_LIBRARY_PATH="$CARGO_TARGET_DIR/release" "$1" ;;
    *) LD_LIBRARY_PATH="$CARGO_TARGET_DIR/release" "$1" ;;
    esac
}
run "$CARGO_TARGET_DIR/weida-c-smoke"
run "$CARGO_TARGET_DIR/weida-c-easy"
