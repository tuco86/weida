#!/bin/sh
# Builds build/CWeida.xcframework, the binary target of the Swift package
# (docs/decisions/0046): static libweida.a for macOS (arm64 and x86_64), iOS
# devices (arm64) and the iOS simulator (arm64 and x86_64), each slice with
# weida.h and the module map of the C module CWeida.
#
#   build-xcframework.sh               every slice
#   build-xcframework.sh --macos-only  the aarch64-apple-darwin slice only
#
# libweida.a is not one of weida-c's crate types (crates/weida-c/Cargo.toml),
# so each slice is built with `cargo rustc --crate-type staticlib`. Needs
# Xcode (xcodebuild, the iOS SDKs ring's C code compiles against) and runs
# from anywhere; the cargo target directory is CARGO_TARGET_DIR or the
# workspace's target/.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
target_dir=${CARGO_TARGET_DIR:-$root/target}
build="$here/build"

macos_only=false
case "${1:-}" in
    "") ;;
    --macos-only) macos_only=true ;;
    *) echo "usage: $0 [--macos-only]" >&2; exit 2 ;;
esac

if $macos_only; then
    targets="aarch64-apple-darwin"
else
    targets="aarch64-apple-darwin x86_64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios"
fi

# shellcheck disable=SC2086 # the target list is split on purpose
rustup target add $targets

# The deployment targets match Package.swift's platforms, so the objects
# ring's C code compiles to do not ask for a newer OS than the package.
export MACOSX_DEPLOYMENT_TARGET=13.0
export IPHONEOS_DEPLOYMENT_TARGET=16.0

for t in $targets; do
    (cd "$root" && cargo rustc --release --locked -p weida-c --lib --crate-type staticlib --target "$t")
done

lib() {
    echo "$target_dir/$1/release/libweida.a"
}

rm -rf "$build/headers" "$build/lib" "$build/CWeida.xcframework"
mkdir -p "$build/headers" "$build/lib/macos" "$build/lib/ios" "$build/lib/ios-simulator"
cp "$root/crates/weida-c/include/weida.h" "$build/headers/"
cat >"$build/headers/module.modulemap" <<'EOF'
module CWeida {
    header "weida.h"
    link "weida"
    export *
}
EOF

if $macos_only; then
    cp "$(lib aarch64-apple-darwin)" "$build/lib/macos/libweida.a"
    xcodebuild -create-xcframework \
        -library "$build/lib/macos/libweida.a" -headers "$build/headers" \
        -output "$build/CWeida.xcframework"
else
    lipo -create "$(lib aarch64-apple-darwin)" "$(lib x86_64-apple-darwin)" \
        -output "$build/lib/macos/libweida.a"
    cp "$(lib aarch64-apple-ios)" "$build/lib/ios/libweida.a"
    lipo -create "$(lib aarch64-apple-ios-sim)" "$(lib x86_64-apple-ios)" \
        -output "$build/lib/ios-simulator/libweida.a"
    xcodebuild -create-xcframework \
        -library "$build/lib/macos/libweida.a" -headers "$build/headers" \
        -library "$build/lib/ios/libweida.a" -headers "$build/headers" \
        -library "$build/lib/ios-simulator/libweida.a" -headers "$build/headers" \
        -output "$build/CWeida.xcframework"
fi
echo "built $build/CWeida.xcframework"
