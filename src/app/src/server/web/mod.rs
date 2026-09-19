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
use rust_embed::{Embed, EmbeddedFile};
use std::borrow::Cow;

/// The bundle produced by `web/`. `allow_missing` keeps the crate compiling
/// when the directory has not been built, which is what happens on a plain
/// `cargo build` checkout without Node: the binary then simply serves no UI
/// and says so on `/`.
#[derive(Embed)]
#[folder = "../../web/dist"]
#[allow_missing = true]
struct WebAssets;

const INDEX_HTML: &str = "index.html";

const MISSING_BUNDLE: &str = "web UI is not bundled in this build; build web/ and recompile";

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

/// One file of the bundle, reduced to what a response needs. Decoupled from
/// `EmbeddedFile` so the response logic can be exercised without a built
/// bundle on disk.
struct Asset {
    data: Cow<'static, [u8]>,
    mimetype: String,
    sha256: [u8; 32],
}

impl From<EmbeddedFile> for Asset {
    fn from(file: EmbeddedFile) -> Self {
        Self {
            mimetype: file.metadata.mimetype().to_string(),
            sha256: file.metadata.sha256_hash(),
            data: file.data,
        }
    }
}

async fn serve_asset(uri: Uri, headers: HeaderMap) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { INDEX_HTML } else { path };

    match respond(WebAssets::get(path).map(Asset::from), &headers) {
        Some(response) => response,
        None if path == INDEX_HTML => (StatusCode::NOT_FOUND, MISSING_BUNDLE).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn respond(asset: Option<Asset>, request_headers: &HeaderMap) -> Option<Response> {
    let asset = asset?;
    let etag = etag(&asset.sha256);

    let if_none_match = request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok());
    if if_none_match == Some(etag.as_str()) {
        return Some(StatusCode::NOT_MODIFIED.into_response());
    }

    let mut response = asset.data.into_owned().into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&asset.mimetype)
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    fn css_asset() -> Asset {
        Asset {
            data: Cow::Borrowed(b"body { margin: 0 }"),
            mimetype: "text/css".to_string(),
            sha256: [0xab; 32],
        }
    }

    fn header(response: &Response, name: header::HeaderName) -> Option<&str> {
        response.headers().get(name).map(|v| v.to_str().unwrap())
    }

    async fn body(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[test]
    fn etag_is_quoted_lowercase_hex() {
        assert_eq!(etag(&[0xab; 32]), format!("\"{}\"", "ab".repeat(32)));
    }

    #[tokio::test]
    async fn unknown_path_is_404_without_body() {
        let response = serve_asset("/no/such/file".parse().unwrap(), HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body(response).await, "");
    }

    #[test]
    fn asset_gets_etag_and_content_type() {
        let response = respond(Some(css_asset()), &HeaderMap::new()).unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(header(&response, header::CONTENT_TYPE), Some("text/css"));
        assert_eq!(
            header(&response, header::ETAG),
            Some(etag(&[0xab; 32]).as_str())
        );
    }

    #[test]
    fn matching_if_none_match_is_304() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_str(&etag(&[0xab; 32])).unwrap(),
        );
        let response = respond(Some(css_asset()), &headers).unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(header(&response, header::CONTENT_TYPE), None);

        // A stale validator must get the full asset again.
        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("\"stale\""));
        let response = respond(Some(css_asset()), &headers).unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn invalid_mimetype_falls_back_to_octet_stream() {
        let asset = Asset {
            mimetype: "text/\nbroken".to_string(),
            ..css_asset()
        };
        let response = respond(Some(asset), &HeaderMap::new()).unwrap();
        assert_eq!(
            header(&response, header::CONTENT_TYPE),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn missing_asset_is_none() {
        assert!(respond(None, &HeaderMap::new()).is_none());
    }

    /// Runs against whatever `web/dist` holds, so it asserts both sides: a
    /// built bundle is served, a missing one is explained rather than 404ing
    /// silently.
    #[tokio::test]
    async fn index_is_served_or_explained() {
        let response = serve_asset("/".parse().unwrap(), HeaderMap::new()).await;
        if is_bundled() {
            assert_eq!(response.status(), StatusCode::OK);
            assert!(
                header(&response, header::CONTENT_TYPE)
                    .unwrap()
                    .starts_with("text/html")
            );
            assert!(body(response).await.contains("<div id=\"root\">"));
        } else {
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert_eq!(body(response).await, MISSING_BUNDLE);
        }
    }
}
