//! Serves the web UI bundle built by `web/`, embedded at compile time.
//!
//! The UI is a single page with no client-side router, so there is no SPA
//! fallback: `/` is the entry point and every other path either names an
//! embedded asset or does not exist. Answering an unknown path with the entry
//! point instead would turn a typo into a 200 and make it hard to tell a
//! missing asset from a working one.
//!
//! The bundle is optional. `web/dist` is built out of band by
//! `pnpm -C web build`, and a checkout without Node simply compiles a binary
//! that has no UI and says so in plain text on `/` — `build.rs` records which
//! of the two it is as `LAKELET_BUILD_WEB_UI`.

mod embed;

use axum::Router;
use axum::routing::get;

/// The body served on `/` when no bundle was embedded. Plain text rather than
/// a placeholder page: there is nothing to render, and the one thing a reader
/// needs is the command that fixes it.
pub(super) const MISSING_BUNDLE: &str =
    "web UI is not bundled in this build; run `pnpm -C web build` and recompile";

/// The same fact as [`MISSING_BUNDLE`], phrased for the one-line server startup
/// banner. Kept beside it so the two cannot drift apart.
const STATUS: &str = "not bundled (web/dist was missing at compile time)";

/// Whether a bundle was embedded. `build.rs` reports it through
/// `LAKELET_BUILD_WEB_UI`, the same `cargo:rustc-env` channel that carries the
/// commit for `--version`.
///
/// A `const` rather than a `#[cfg]` so both arms of every caller stay compiled
/// and linted no matter which kind of build this is; the branch still folds
/// away at compile time.
pub(super) const IS_BUNDLED: bool = matches!(env!("LAKELET_BUILD_WEB_UI").as_bytes(), b"1");

/// Where the UI is reachable, or why it is not. Phrased for the one line the
/// server prints on startup.
pub(super) fn status_line(port: u16) -> String {
    if IS_BUNDLED {
        format!("http://localhost:{port}/")
    } else {
        STATUS.to_string()
    }
}

/// Routes every path to the embedded bundle. Meant to be registered as a
/// fallback by the caller so it never shadows a gRPC service mounted at its
/// own path prefix.
pub(super) fn router() -> Router {
    Router::new().fallback(get(embed::serve_asset))
}
