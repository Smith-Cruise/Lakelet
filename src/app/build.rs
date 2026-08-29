//! Embeds the commit this binary was built from, surfaced by `lakelet --version`.
//!
//! Lakelet has no release versioning: every build reports the same
//! `CARGO_PKG_VERSION`, so the commit is the only thing that identifies a
//! binary. Missing git metadata (a source tarball, or no git on the machine)
//! degrades to "an unknown commit" rather than failing the build.

use std::path::PathBuf;
use std::process::Command;

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
}

/// Rerun only when the checked-out commit moves. Emitting any
/// `rerun-if-changed` replaces cargo's default "rerun on any change inside the
/// package" rule, which is what we want: this script's output depends on git
/// state alone, and normal recompilation of `src/**` is unaffected.
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
