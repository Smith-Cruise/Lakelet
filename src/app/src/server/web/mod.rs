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
//! that has no UI and says so in plain text on `/`.

mod embed;

use axum::Router;
use axum::routing::get;

/// Whether the bundle can be served right now; see [`embed::has_bundle`].
/// Only the server's own tests reach for it: [`status_line`] is the one
/// caller in a normal build, and it goes straight to `embed`.
#[cfg(test)]
pub(super) use embed::has_bundle;

/// The body served on `/` when there is no bundle to serve. Plain text rather
/// than a placeholder page: there is nothing to render, and the one thing a
/// reader needs is the command that fixes it.
///
/// Recompiling is named as the release-only step it is: a debug build reads
/// `web/dist` when the request arrives, so building the bundle is enough.
pub(super) const MISSING_BUNDLE: &str =
    "web UI is not available; run `pnpm -C web build`, then recompile for a release build";

/// The same fact as [`MISSING_BUNDLE`], phrased for the one-line server startup
/// banner. Kept beside it so the two cannot drift apart.
const STATUS: &str = "not served (no bundle in web/dist; run `pnpm -C web build`)";

/// Where the UI is reachable, or why it is not. Phrased for the one line the
/// server prints on startup.
///
/// Announcing a URL that answers 404 is the one thing this line must never do,
/// so it asks the assets. Whether a bundle existed when the binary was
/// compiled is a different question and not the one worth answering here: a
/// debug build reads `web/dist` at runtime, so that answer can already be
/// stale by the time the server starts.
pub(super) fn status_line(port: u16) -> String {
    if embed::has_bundle() {
        format!("http://localhost:{port}")
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
