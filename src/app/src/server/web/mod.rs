//! Serves the embedded web UI from the Flight SQL port.
//!
//! The UI is a single page with no client-side router, so there is no SPA
//! fallback: `/` is the entry point and every other path either names an
//! embedded asset or does not exist. Answering an unknown path with the entry
//! point instead would turn a typo into a 200 and make it hard to tell a
//! missing asset from a working one.

use axum::Router;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use rust_embed::Embed;

/// The bundle produced by `web/`. `allow_missing` keeps the crate compiling
/// when the directory has not been built, which is what happens on a plain
/// `cargo clippy --all-features` checkout: the binary then simply serves no UI.
#[derive(Embed)]
#[folder = "../../web/dist"]
#[allow_missing = true]
struct WebAssets;

const INDEX_HTML: &str = "index.html";

const MISSING_BUNDLE: &str =
    "web UI is not bundled in this build; build web/ and recompile with --features web-ui";

/// Whether a bundle was embedded (or, in debug builds, is on disk right now).
pub fn is_bundled() -> bool {
    WebAssets::get(INDEX_HTML).is_some()
}

/// Routes every non-gRPC path to the embedded bundle. Registered as a fallback
/// rather than explicit routes so it never shadows the Flight service, which
/// `Routes::add_service` mounts at its own path prefix.
pub fn router() -> Router {
    Router::new().fallback(get(serve_asset))
}

async fn serve_asset(uri: Uri, headers: HeaderMap) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { INDEX_HTML } else { path };

    match respond(path, &headers) {
        Some(response) => response,
        None if path == INDEX_HTML => (StatusCode::NOT_FOUND, MISSING_BUNDLE).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn respond(path: &str, request_headers: &HeaderMap) -> Option<Response> {
    let file = WebAssets::get(path)?;
    let etag = etag(&file.metadata.sha256_hash());

    let if_none_match = request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok());
    if if_none_match == Some(etag.as_str()) {
        return Some(StatusCode::NOT_MODIFIED.into_response());
    }

    let mimetype = file.metadata.mimetype().to_string();
    let mut response = file.data.into_owned().into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&mimetype)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    if let Ok(value) = HeaderValue::from_str(&etag) {
        headers.insert(header::ETAG, value);
    }
    Some(response)
}

fn etag(hash: &[u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut quoted = String::with_capacity(hash.len() * 2 + 2);
    quoted.push('"');
    for byte in hash {
        let _ = write!(quoted, "{byte:02x}");
    }
    quoted.push('"');
    quoted
}
