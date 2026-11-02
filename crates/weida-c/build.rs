//! Moves the DLL's PDB out of the way of the `weida` CLI's.
//!
//! On Windows the library is `weida.dll`, and MSVC's linker writes its PDB
//! beside it as `deps/weida.pdb`, the file the `weida` CLI's link writes too:
//! two links at once fail with LNK1201. rustc passes `/DEBUG` to MSVC's
//! linker whatever `strip` says, so the DLL's PDB is named into this build
//! script's own OUT_DIR instead.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        let out = std::env::var("OUT_DIR").expect("cargo sets OUT_DIR for a build script");
        let pdb = Path::new(&out).join("weida.pdb");
        println!("cargo:rustc-cdylib-link-arg=/PDB:{}", pdb.display());
    }
}
