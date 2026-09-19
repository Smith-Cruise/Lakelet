//! Keeps `web/dist` present and tells the crate whether it holds a bundle.
//!
//! The bundle is built out of band by `pnpm -C web build`; nothing here runs
//! Node, so a plain `cargo build` needs no frontend toolchain. What this script
//! does is remove the three ways that arrangement used to fail silently:
//! a half-built `web/dist` embedding zero files, a freshly built bundle that
//! never triggered a recompile, and tests whose outcome depended on whatever
//! happened to be on disk.

use std::path::{Path, PathBuf};

/// Relative to `CARGO_MANIFEST_DIR` (`src/web`), and the same path the
/// `#[folder]` attribute in `src/embed.rs` points at.
const DIST: &str = "../../web/dist";

fn main() {
    // `web_ui_bundled` is set below rather than by a feature; declaring it
    // keeps `unexpected_cfgs` quiet, and CI builds with `-D warnings`.
    println!("cargo:rustc-check-cfg=cfg(web_ui_bundled)");

    let dist =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets this")).join(DIST);

    // Creating the directory is what lets the rest work: rust-embed's
    // `#[folder]` always resolves (so it needs no `allow_missing`), and the
    // `rerun-if-changed` below always names an existing path. Cargo treats a
    // missing `rerun-if-changed` path as perpetually changed, which would
    // rerun this script — and rebuild every dependent crate — on every build.
    if let Err(e) = std::fs::create_dir_all(&dist) {
        panic!(
            "failed to create {}: {e}\n\
             The web UI bundle directory has to exist for this crate to \
             compile, whether or not it holds a bundle. Create it, or run \
             `pnpm -C web build` to fill it.",
            dist.display()
        );
    }

    // Cargo walks the directory, so a rebuilt bundle reruns this script even
    // when the change is a *new* file. rust-embed cannot cover that case on
    // its own: its release output is `include_bytes!` per file, which rustc
    // tracks only for files that already existed.
    println!("cargo:rerun-if-changed={}", dist.display());

    if dist.join("index.html").is_file() {
        println!("cargo:rustc-cfg=web_ui_bundled");
    } else if has_entries(&dist) {
        // An empty directory is the ordinary "no bundle" case. Files without
        // an entry point mean a `vite build` that died halfway or a truncated
        // CI artifact, which would otherwise ship a UI-less binary in silence.
        println!("cargo:warning=web/dist has no index.html; the web UI will not be served");
    }
}

fn has_entries(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some())
}
