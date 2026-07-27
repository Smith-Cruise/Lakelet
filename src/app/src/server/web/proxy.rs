//! Reverse proxy that serves the hosted web UI from a local port.
//!
//! The UI listener's fallback: any path that is not an API route is fetched
//! from the hosted static site (ui.lakelet.dev) and streamed back, so the
//! browser sees the UI and the API on one same-origin localhost address and
//! no CORS is involved.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::sync::Arc;

/// The hosted web UI (a static Cloudflare Worker site, see `web-ui/`).
pub const DEFAULT_UI_UPSTREAM: &str = "https://ui.lakelet.dev";

/// Environment variable overriding the upstream, for pointing the proxy at a
/// local `vite preview` or a staging deployment during development.
pub const UI_UPSTREAM_ENV: &str = "LAKELET_UI_URL";

/// Request headers forwarded to the upstream. An allowlist, not a denylist:
/// everything else (host, cookie, authorization, origin, referer, ...) is
/// dropped so nothing local leaks to the CDN. The conditional headers let the
/// browser revalidate its cache (304s pass back through untouched).
const FORWARD_REQUEST_HEADERS: [HeaderName; 6] = [
    header::ACCEPT,
    header::ACCEPT_ENCODING,
    header::ACCEPT_LANGUAGE,
    header::IF_NONE_MATCH,
    header::IF_MODIFIED_SINCE,
    header::RANGE,
];

/// Response headers forwarded back to the browser. Hop-by-hop headers and CDN
/// noise (set-cookie, cf-*) never get copied because they are not listed.
const FORWARD_RESPONSE_HEADERS: [HeaderName; 9] = [
    header::CONTENT_TYPE,
    header::CONTENT_ENCODING,
    header::CONTENT_LENGTH,
    header::CACHE_CONTROL,
    header::ETAG,
    header::LAST_MODIFIED,
    header::VARY,
    header::ACCEPT_RANGES,
    header::CONTENT_RANGE,
];

#[derive(Clone)]
pub struct ProxyState {
    client: reqwest::Client,
    /// Upstream base URL without a trailing slash.
    upstream: Arc<str>,
}

impl ProxyState {
    pub fn new(upstream: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            upstream: upstream.trim_end_matches('/').into(),
        }
    }

    pub fn from_env() -> Self {
        Self::new(&Self::upstream_override().unwrap_or_else(|| DEFAULT_UI_UPSTREAM.to_string()))
    }

    pub fn upstream_override() -> Option<String> {
        std::env::var(UI_UPSTREAM_ENV)
            .ok()
            .filter(|s| !s.is_empty())
    }
}

/// Fallback handler on the web UI listener: proxies GET/HEAD to the hosted
/// static site and streams the response back. No local caching; the upstream's
/// cache headers pass through so the browser caches the content-hashed assets.
pub async fn proxy_ui(State(proxy): State<ProxyState>, req: Request) -> Response {
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::ALLOW, "GET, HEAD")],
            "the web UI proxy only serves GET and HEAD",
        )
            .into_response();
    }

    // Keep the query string: the SPA may use it, and the upstream Worker
    // answers unknown paths with index.html itself, so no fallback logic here.
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let url = format!("{}{}", proxy.upstream, path_and_query);

    let mut upstream_req = proxy.client.request(req.method().clone(), &url);
    for name in FORWARD_REQUEST_HEADERS {
        if let Some(value) = req.headers().get(&name) {
            upstream_req = upstream_req.header(name, value);
        }
    }

    let upstream_resp = match upstream_req.send().await {
        Ok(resp) => resp,
        Err(err) => return offline_response(&proxy.upstream, &err),
    };

    let mut builder = Response::builder().status(upstream_resp.status());
    for name in FORWARD_RESPONSE_HEADERS {
        if let Some(value) = upstream_resp.headers().get(&name) {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Body::from_stream(upstream_resp.bytes_stream()))
        .expect("allowlisted headers and upstream status are valid response parts")
}

/// The UI cannot be served without reaching the upstream; say so instead of
/// showing the browser a bare connection error.
fn offline_response(upstream: &str, err: &reqwest::Error) -> Response {
    let detail = html_escape(&err.to_string());
    let upstream = html_escape(upstream);
    let page = format!(
        r#"<!doctype html>
<html>
<head><meta charset="utf-8"><title>Lakelet - web UI unavailable</title></head>
<body style="font-family: system-ui, sans-serif; max-width: 40rem; margin: 4rem auto; line-height: 1.5">
<h1>Web UI unavailable</h1>
<p>Lakelet could not fetch the web UI from <code>{upstream}</code>.
Serving the UI requires an internet connection &mdash; check your network and reload.
The REST API is unaffected.</p>
<pre style="white-space: pre-wrap; color: #666">{detail}</pre>
</body>
</html>
"#
    );
    (
        StatusCode::BAD_GATEWAY,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        page,
    )
        .into_response()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::routing::get;
    use tower::ServiceExt;

    /// Serve `mock` on an ephemeral loopback port and return its base URL.
    async fn spawn_upstream(mock: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, mock).into_future());
        format!("http://{addr}")
    }

    fn proxy_router(upstream: &str) -> Router {
        use axum::handler::Handler;
        Router::new().fallback_service(proxy_ui.with_state(ProxyState::new(upstream)))
    }

    fn get_request(uri: &str) -> Request {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    async fn body_string(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn passes_through_body_content_type_and_query() {
        let mock = Router::new()
            .route(
                "/",
                get(|| async {
                    (
                        [
                            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                            (header::CACHE_CONTROL, "public, max-age=60"),
                        ],
                        "<html>ui</html>",
                    )
                }),
            )
            .route(
                "/assets/x.js",
                get(|req: Request| async move {
                    (
                        [(header::CONTENT_TYPE, "text/javascript")],
                        format!("query={}", req.uri().query().unwrap_or("")),
                    )
                }),
            );
        let app = proxy_router(&spawn_upstream(mock).await);

        let response = app.clone().oneshot(get_request("/")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "public, max-age=60"
        );
        assert_eq!(body_string(response).await, "<html>ui</html>");

        let response = app.oneshot(get_request("/assets/x.js?v=1")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_string(response).await, "query=v=1");
    }

    #[tokio::test]
    async fn unreachable_upstream_returns_bad_gateway_html() {
        // Bind then drop a listener so the port is (almost certainly) closed.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let response = proxy_router(&upstream)
            .oneshot(get_request("/"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert!(body_string(response).await.contains("internet connection"));
    }

    #[tokio::test]
    async fn non_get_is_method_not_allowed_without_touching_upstream() {
        // Unroutable upstream: reaching it would fail the test with a 502.
        let response = proxy_router("http://127.0.0.1:1")
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn strips_sensitive_request_headers() {
        // The mock echoes whether the allowlist did its job.
        let mock = Router::new().route(
            "/",
            get(|req: Request| async move {
                let has = |name: HeaderName| req.headers().contains_key(name);
                format!(
                    "cookie={} authorization={} accept-language={}",
                    has(header::COOKIE),
                    has(header::AUTHORIZATION),
                    has(header::ACCEPT_LANGUAGE),
                )
            }),
        );
        let app = proxy_router(&spawn_upstream(mock).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(header::COOKIE, "session=secret")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .header(header::ACCEPT_LANGUAGE, "en")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            body_string(response).await,
            "cookie=false authorization=false accept-language=true"
        );
    }

    #[tokio::test]
    async fn api_routes_win_over_the_proxy_fallback() {
        use axum::handler::Handler;
        // The UI listener mounts API routes plus the proxy fallback; a local
        // route must never be forwarded upstream.
        let mock = Router::new().fallback(|| async { "from upstream" });
        let upstream = spawn_upstream(mock).await;
        let app = Router::new()
            .route("/api/info", get(|| async { "local api" }))
            .fallback_service(proxy_ui.with_state(ProxyState::new(&upstream)));

        let response = app.clone().oneshot(get_request("/api/info")).await.unwrap();
        assert_eq!(body_string(response).await, "local api");

        let response = app.oneshot(get_request("/anything")).await.unwrap();
        assert_eq!(body_string(response).await, "from upstream");
    }
}
