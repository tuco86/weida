// swift-tools-version: 6.0
// weida for Swift (docs/libraries/weida-swift.md): the module Weida over
// the C module CWeida, an XCFramework of static libweida.a that
// build-xcframework.sh builds into build/.

import PackageDescription

let package = Package(
    name: "Weida",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "Weida", targets: ["Weida"]),
    ],
    targets: [
        .binaryTarget(name: "CWeida", path: "build/CWeida.xcframework"),
        // The native libraries libweida.a needs, as
        // `cargo rustc -p weida-c --lib --crate-type staticlib -- --print native-static-libs`
        // names them; libSystem, libc and libm are linked anyway.
        .target(
            name: "Weida",
            dependencies: ["CWeida"],
            linkerSettings: [
                .linkedFramework("Security"),
                .linkedFramework("CoreFoundation"),
                .linkedLibrary("iconv"),
            ]
        ),
        .testTarget(name: "WeidaTests", dependencies: ["Weida", "CWeida"]),
    ]
)
