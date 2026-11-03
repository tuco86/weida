#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "nightly"]
# platform = "darwin-arm64"
# timeout_minutes = 60
# ///
# weida for Swift (docs/libraries/weida-swift.md) on atik's host, which has
# Xcode. A push builds the XCFramework's aarch64-apple-darwin slice and runs
# the unit tests; the nightly and a release tag build every slice (macOS,
# iOS devices, the iOS simulator), run the integration tests too and build
# the test bundle for iOS and the simulator, which links libweida.a into it
# and so proves those slices link. No iOS device or simulator runs the
# tests. Needs Xcode with the iOS platform (`xcodebuild -downloadPlatform
# iOS`), which a generic iOS destination asks for.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env

package=crates/swift/weida
case $(tier) in
    ci)
        step "$package/build-xcframework.sh"
        export WEIDA_INTEGRATION=1
        step swift test --package-path "$package"
        for destination in "generic/platform=iOS" "generic/platform=iOS Simulator"; do
            (cd "$package" && step xcodebuild build-for-testing -quiet -scheme Weida \
                -destination "$destination" \
                -derivedDataPath "$CI_WORKSPACE/.ci-cache/swift-derived-data" \
                CODE_SIGNING_ALLOWED=NO)
        done
        ;;
    *)
        step "$package/build-xcframework.sh" --macos-only
        step swift test --package-path "$package"
        ;;
esac
