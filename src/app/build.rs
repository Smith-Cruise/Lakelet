//! Build-time inputs for the binary: the commit it was built from, and the
//! web UI bundle it embeds.
//!
//! Lakelet has no release versioning: every build reports the same
//! `CARGO_PKG_VERSION`, so the commit is the only thing that identifies a
//! binary. Missing git metadata (a source tarball, or no git on the machine)
//! degrades to "an unknown commit" rather than failing the build.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Relative to `CARGO_MANIFEST_DIR` (`src/app`), and the same path the
/// `#[folder]` attribute in `src/server/web/embed.rs` points at.
const DIST: &str = "../../web/dist";

fn main() {
    let sha = git(&["rev-parse", "--short", "HEAD"]);
    // `format-local` honours TZ, so the timestamp reads the same no matter
    // which timezone the build machine sits in.
    let commit_time = git(&[
        "log",
        "-1",
        "--format=%cd",
        "--date=format-local:%Y-%m-%d %H:%M:%S UTC",
    ]);

    let provenance = match (sha, commit_time) {
        (Some(sha), Some(commit_time)) => format!("{sha} {commit_time}"),
        (Some(sha), None) => sha,
        _ => "an unknown commit".to_string(),
    };
    println!("cargo:rustc-env=LAKELET_BUILD_PROVENANCE={provenance}");

    emit_rerun_triggers();
    prepare_web_bundle();
}

/// Rerun when the checked-out commit moves. Emitting any `rerun-if-changed`
/// replaces cargo's default "rerun on any change inside the package" rule,
/// which is what we want: this script's inputs are git state and `web/dist`
/// (see `prepare_web_bundle`), neither of which cargo would watch otherwise,
/// and normal recompilation of `src/**` is unaffected.
fn emit_rerun_triggers() {
    let head_ref = git(&["rev-parse", "--symbolic-full-name", "HEAD"]);
    for path in ["HEAD", "packed-refs"] {
        // `--git-path` resolves against the real git dir, so linked worktrees
        // (where .git is a file) point at the right place.
        let Some(resolved) = git(&["rev-parse", "--git-path", path]) else {
            continue;
        };
        // A path that does not exist is treated by cargo as perpetually
        // changed, which would rerun this script — and rebuild the crate — on
        // every single build.
        if PathBuf::from(&resolved).exists() {
            println!("cargo:rerun-if-changed={resolved}");
        }
    }

    // Detached HEAD reports the raw sha instead of a ref name; there is no
    // ref file to watch in that case.
    let Some(head_ref) = head_ref.filter(|r| r.starts_with("refs/")) else {
        return;
    };
    let Some(resolved) = git(&["rev-parse", "--git-path", &head_ref]) else {
        return;
    };
    let resolved = PathBuf::from(resolved);
    if resolved.exists() {
        println!("cargo:rerun-if-changed={}", resolved.display());
        return;
    }

    // A branch that exists only in packed-refs has no loose ref file yet.
    // Watch the nearest existing parent so creating the loose ref on the next
    // commit reruns this script and refreshes the embedded provenance.
    if let Some(parent) = resolved.ancestors().skip(1).find(|path| path.exists()) {
        println!("cargo:rerun-if-changed={}", parent.display());
    }
}

/// Keeps `web/dist` present so the crate always compiles, and complains about
/// a bundle that is there but broken.
///
/// The bundle is built out of band by `pnpm -C web build`; nothing here runs
/// Node, so a plain `cargo build` needs no frontend toolchain. What this does
/// is remove two ways that arrangement used to fail silently: a half-built
/// `web/dist` embedding zero files, and a freshly built bundle that never
/// triggered a recompile.
fn prepare_web_bundle() {
    let dist =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets this")).join(DIST);

    // Creating the directory is what lets the rest work: rust-embed's
    // `#[folder]` always resolves (so it needs no `allow_missing`), and the
    // `rerun-if-changed` below always names an existing path. Cargo treats a
    // missing `rerun-if-changed` path as perpetually changed, which would
    // rerun this script — and rebuild the crate — on every build.
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

    // Whether a bundle was here at compile time is deliberately not reported
    // to the crate: a debug build reads `web/dist` at runtime, so the answer
    // can be stale by the time the server starts. The one place that cares
    // asks the assets instead — see `server::web::embed::has_bundle`.
    let bundled = dist.join("index.html").is_file();

    if !bundled && has_entries(&dist) {
        // An empty directory is the ordinary "no bundle" case. Files without
        // an entry point mean a `vite build` that died halfway or a truncated
        // CI artifact, which would otherwise ship a UI-less binary in silence.
        println!("cargo:warning=web/dist has no index.html; the web UI will not be served");
    }
}

fn has_entries(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some())
}

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .env("TZ", "UTC")
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}
