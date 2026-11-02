//! Linker arguments for libweida's cdylib that rustc does not pass itself.
//!
//! On Windows the library is `weida.dll`, and MSVC's linker writes its PDB
//! beside it as `deps/weida.pdb`, the file the `weida` CLI's link writes too:
//! two links at once fail with LNK1201. rustc passes `/DEBUG` to MSVC's
//! linker whatever `strip` says, so the DLL's PDB is named into this build
//! script's own OUT_DIR instead.
//!
//! On macOS the dylib's install name is set to `@rpath/libweida.dylib`.
//! Without this, rustc leaves it at its absolute build path, so a library
//! linked against it (the weida-js addon) looks for it there instead of
//! beside itself.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        let out = std::env::var("OUT_DIR").expect("cargo sets OUT_DIR for a build script");
        let pdb = Path::new(&out).join("weida.pdb");
        println!("cargo:rustc-cdylib-link-arg=/PDB:{}", pdb.display());
    } else if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/libweida.dylib");
    }
}
