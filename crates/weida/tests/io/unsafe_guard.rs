//! One guard for every crate that allows `unsafe`, and for every crate that
//! says nothing about it.
//!
//! Most crates here inherit `unsafe_code = "forbid"` from the workspace lint
//! table and are done. Six cannot:
//! a PyO3 crate's `#[pyclass]`, `#[pymethods]` and `#[pyfunction]` expand to
//! `unsafe impl`s of PyO3's type-object traits, and the lint does not care
//! that the code came from a macro. So those manifests set
//! `unsafe_code = "allow"`, each under a comment asserting that **no `unsafe`
//! is written by hand in this crate**, and this test is what stands behind
//! that claim for every one of them. (`weida-winpipe`, the Win32 FFI crate,
//! is the one crate that writes `unsafe` by hand on purpose; its manifest
//! says so and narrows the lint to `unsafe_op_in_unsafe_fn`.)
//!
//! The shape matters more than the coverage. This is **one test that walks the
//! manifests** rather than seven copies of one test, so a crate that gains the
//! allowance tomorrow is covered by the fact of having it: nobody has to
//! remember to copy a file. The same walk catches the other way a guarantee
//! goes quiet — a crate whose manifest says *nothing*, which permits `unsafe`
//! by default and claims nothing at all.
//!
//! Two deliberate exclusions:
//!
//! * **`fuzz/` crates.** Each declares its own `[workspace]`, so no
//!   workspace-wide build ever compiles them and no workspace lint applies;
//!   their manifests are `cargo-fuzz`'s shape rather than this repository's.
//! * **Generated code and dependencies.** The walk reads `src` only.

use std::fs;
use std::path::{Path, PathBuf};

/// What a manifest says about `unsafe`.
#[derive(Debug, PartialEq, Eq)]
enum Stance {
    Forbid,
    Allow,
    /// Forbids only `unsafe_op_in_unsafe_fn`: the Win32 FFI crate.
    Ffi,
    Silent,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root is two directories up from this crate")
}

/// Every crate manifest under `crates/`, excluding the self-contained `fuzz`
/// workspaces.
fn manifests(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("a readable directory") {
            let path = entry.expect("a readable directory entry").path();
            if path.is_dir() {
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if name == "target" || name == "fuzz" {
                    continue;
                }
                stack.push(path);
            } else if path.file_name().is_some_and(|name| name == "Cargo.toml") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// Whether a manifest opts into the workspace lint table (`[lints]` followed
/// by `workspace = true`).
fn inherits_workspace_lints(text: &str) -> bool {
    let mut lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'));
    while let Some(line) = lines.next() {
        if line == "[lints]" {
            return lines.next() == Some("workspace = true");
        }
    }
    false
}

fn stance(manifest: &Path) -> Stance {
    let text = fs::read_to_string(manifest).expect("a readable manifest");
    let root = fs::read_to_string(workspace_root().join("Cargo.toml")).expect("the root manifest");
    if text.contains(r#"unsafe_code = "allow""#) {
        Stance::Allow
    } else if text.contains(r#"unsafe_code = "forbid""#)
        || (inherits_workspace_lints(&text) && root.contains(r#"unsafe_code = "forbid""#))
    {
        Stance::Forbid
    } else if text.contains(r#"unsafe_op_in_unsafe_fn = "forbid""#) {
        Stance::Ffi
    } else {
        Stance::Silent
    }
}

/// Every `.rs` file under `dir`, recursively, so a module in a subdirectory is
/// checked as well.
fn sources(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let path = entry.expect("a readable directory entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// Lines that write `unsafe` by hand, as `path:line: text`.
///
/// Line comments are skipped because the word appears in prose — this file's
/// own documentation explains what PyO3's macros expand to, and a doc comment
/// is not code.
fn hand_written_unsafe(file: &Path) -> Vec<String> {
    let source = fs::read_to_string(file).expect("readable Rust source");
    let mut offenders = Vec::new();
    for (number, line) in source.lines().enumerate() {
        let code = line.trim_start();
        if code.starts_with("//") {
            continue;
        }
        if code.contains("unsafe ") || code.contains("unsafe{") {
            offenders.push(format!("{}:{}: {}", file.display(), number + 1, code));
        }
    }
    offenders
}

/// The claim six manifests make, tested once for all of them.
#[test]
fn no_crate_that_allows_unsafe_writes_any_by_hand() {
    let root = workspace_root();
    let allowing: Vec<PathBuf> = manifests(&root)
        .into_iter()
        .filter(|manifest| stance(manifest) == Stance::Allow)
        .collect();

    assert!(
        allowing.len() >= 6,
        "the six PyO3 crates set the allowance; this walk found {}: {allowing:?}",
        allowing.len()
    );

    let mut scanned = 0usize;
    let mut offenders = Vec::new();
    for manifest in &allowing {
        let src = manifest
            .parent()
            .expect("a manifest has a directory")
            .join("src");
        let files = sources(&src);
        assert!(
            !files.is_empty(),
            "{} allows unsafe and has no source to check",
            manifest.display()
        );
        scanned += files.len();
        for file in files {
            offenders.extend(hand_written_unsafe(&file));
        }
    }

    assert!(
        scanned > 0,
        "no Rust source was scanned, so nothing was checked"
    );
    assert!(
        offenders.is_empty(),
        "unsafe is written by hand in {} crate(s) whose manifests allow it only for PyO3's \
         macro expansion:\n{}",
        allowing.len(),
        offenders.join("\n")
    );
}

/// The other way the guarantee goes quiet: a manifest that says nothing.
///
/// `unsafe_code` defaults to `allow`, so a crate with no `[lints.rust]` stance
/// permits hand-written `unsafe` and claims nothing — which is worse than the
/// six above, because there is no comment to falsify. Every crate in this
/// tree takes a position; this is what keeps that true as crates are added.
#[test]
fn every_crate_takes_a_position_on_unsafe() {
    let root = workspace_root();
    let silent: Vec<PathBuf> = manifests(&root)
        .into_iter()
        .filter(|manifest| stance(manifest) == Stance::Silent)
        .collect();
    assert!(
        silent.is_empty(),
        "these manifests neither forbid nor allow `unsafe_code`, so they permit it by \
         default and say nothing:\n{}",
        silent
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join("\n")
    );
}
