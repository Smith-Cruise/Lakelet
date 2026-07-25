//! HTTP API server behind `lakelet --web-ui`.
//!
//! The binary serves the API only; the web UI is a separately hosted static
//! site (see `web-ui/`) that connects to this server from the browser.
//!
//! # REST API
//!
//! Base URL: `http://127.0.0.1:<web-ui-port>` (loopback only, no authentication).
//!
//! ## `GET /api/info`
//!
//! Server metadata; also used by the web UI as a connectivity probe.
//!
//! Response `200 OK`, `application/json`:
//!
//! ```json
//! { "version": "0.1.0", "default_catalog": null, "default_schema": null }
//! ```
//!
//! ## `POST /api/query`
//!
//! Execute a single SQL statement.
//!
//! Request body, `application/json`:
//!
//! ```json
//! { "sql": "select 1", "catalog": "my_catalog", "schema": "my_schema" }
//! ```
//!
//! - `sql` (required): one SQL statement; a trailing semicolon is accepted.
//! - `catalog` / `schema` (optional): default catalog/schema for resolving
//!   unqualified table names in this request. Sessions are per-request, so
//!   `USE` does not persist; pass these fields (or fully qualify names) instead.
//!
//! Success: `200 OK` with `Content-Type: application/vnd.apache.arrow.stream`.
//! The body is an Arrow IPC stream (schema message first, then record batches),
//! sent chunked as batches are produced. An error after streaming has started
//! aborts the connection, which the client sees as a truncated IPC stream.
//!
//! Failure before streaming: `application/json` body
//! `{ "error": "<message>", "code": "<code>" }` with status:
//!
//! | Status | `code`                | Meaning                              |
//! |--------|-----------------------|--------------------------------------|
//! | 400    | `invalid_sql`         | Parse/plan/schema/config error       |
//! | 501    | `not_implemented`     | Statement not supported              |
//! | 503    | `resources_exhausted` | Memory limit exceeded                |
//! | 500    | `internal`            | Anything else                        |
//!
//! ## Any other path
//!
//! `404 Not Found` with the same JSON error shape and code `not_found`.
//!
//! ## CORS
//!
//! All origins are allowed, and preflights answer Chrome's Private Network
//! Access check (`Access-Control-Allow-Private-Network: true`), so a UI
//! hosted anywhere can call this API cross-origin.
//!
//! Note what this means: binding loopback stops other hosts from connecting
//! directly, but not a browser on this machine. While `--web-ui` runs, any
//! page the user visits can query this API and read the results. Run it only
//! on machines and catalogs where that is acceptable.

use crate::context::LakeletContext;
use crate::sql::session::ExtendedSessionContext;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::io::IsTerminal;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

#[derive(Clone)]
struct AppState {
    lakelet_context: Arc<LakeletContext>,
    runtime_env: Arc<RuntimeEnv>,
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
    #[serde(default)]
    catalog: Option<String>,
    #[serde(default)]
    schema: Option<String>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    code: &'static str,
}

pub async fn serve(
    lakelet_context: Arc<LakeletContext>,
    runtime_env: Arc<RuntimeEnv>,
    port: u16,
) -> Result<()> {
    // Localhost only: the API has no authentication, so it must not be
    // reachable from other hosts. This does not make it private to this
    // process: with the any-origin CORS policy below (needed by the
    // separately hosted UI), any page in a browser on this machine can call
    // it too. See the module docs.
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| DataFusionError::Configuration(format!("Failed to bind {addr}: {e}")))?;
    let app = router(AppState {
        lakelet_context,
        runtime_env,
    });
    println!("Lakelet web UI API listening on http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/query", post(query))
        .route("/api/info", get(info))
        .fallback(fallback)
        .layer(cors_layer())
        .with_state(state)
}

fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any) // the UI can be hosted anywhere; see the module docs
        .allow_methods(Any)
        .allow_headers(Any) // content-type: application/json triggers a preflight
        .allow_private_network(true) // Chrome Private Network Access preflight
        .max_age(Duration::from_secs(3600))
}

async fn info(State(state): State<AppState>) -> Response {
    let body = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "default_catalog": state.lakelet_context.default_catalog,
        "default_schema": state.lakelet_context.default_schema,
    });
    axum::Json(body).into_response()
}

async fn query(
    State(state): State<AppState>,
    axum::Json(request): axum::Json<QueryRequest>,
) -> Response {
    log_executing(&request.sql);
    // A fresh session per request, for the same reason as the flight server:
    // `create_dataframe` swaps the session's catalog list per query.
    // Per-request catalog/schema land on a cloned context (cheap: the heavy
    // parts are behind Arcs) so the shared one stays untouched.
    let mut lakelet_context = (*state.lakelet_context).clone();
    if request.catalog.is_some() {
        lakelet_context.default_catalog = request.catalog;
    }
    if request.schema.is_some() {
        lakelet_context.default_schema = request.schema;
    }
    let session = ExtendedSessionContext::new(Arc::new(lakelet_context), state.runtime_env.clone());

    // Plan (and error out) before the response starts streaming, so SQL
    // errors still surface as JSON with a proper status code.
    let dataframe = match session.sql(&request.sql).await {
        Ok(dataframe) => dataframe,
        Err(err) => return df_error_to_response(err),
    };
    let schema: SchemaRef = Arc::new(dataframe.schema().as_arrow().clone());
    let batches = match dataframe.execute_stream().await {
        Ok(batches) => batches,
        Err(err) => return df_error_to_response(err),
    };

    (
        [(header::CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)],
        ipc_body(schema, batches),
    )
        .into_response()
}

/// Bridge a DataFusion record batch stream into an Arrow IPC stream body.
/// One `StreamWriter` lives for the whole response so dictionary tracking
/// spans batches; its `Vec<u8>` buffer is drained into a chunk after every
/// write. A mid-stream error aborts the connection, which the client sees
/// as a truncated IPC stream.
fn ipc_body(schema: SchemaRef, mut batches: SendableRecordBatchStream) -> Body {
    let stream: futures::stream::BoxStream<'static, std::result::Result<Bytes, ArrowError>> =
        Box::pin(async_stream::try_stream! {
        let mut writer = StreamWriter::try_new(Vec::new(), &schema)?;
        yield Bytes::from(std::mem::take(writer.get_mut()));
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            writer.write(&batch)?;
            yield Bytes::from(std::mem::take(writer.get_mut()));
        }
            writer.finish()?;
            yield Bytes::from(std::mem::take(writer.get_mut()));
        });
    Body::from_stream(stream)
}

fn df_error_to_response(err: DataFusionError) -> Response {
    // Planning errors reach us wrapped in `Context`/`External`, so classify
    // the root cause rather than the wrapper.
    let (status, code) = match err.find_root() {
        DataFusionError::Plan(_)
        | DataFusionError::SQL(..)
        | DataFusionError::SchemaError(..)
        | DataFusionError::Configuration(_) => (StatusCode::BAD_REQUEST, "invalid_sql"),
        DataFusionError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, "not_implemented"),
        DataFusionError::ResourcesExhausted(_) => {
            (StatusCode::SERVICE_UNAVAILABLE, "resources_exhausted")
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    (
        status,
        axum::Json(ErrorBody {
            error: err.to_string(),
            code,
        }),
    )
        .into_response()
}

async fn fallback() -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(ErrorBody {
            error: "Lakelet serves the HTTP API only (POST /api/query, GET /api/info); \
                    the web UI is hosted separately"
                .to_string(),
            code: "not_found",
        }),
    )
        .into_response()
}

fn log_executing(sql: &str) {
    // Strip control characters: the SQL is attacker-controllable, and escape
    // sequences would otherwise let it forge terminal output.
    let sql: String = sql
        .chars()
        .map(|c| match c {
            '\n' | '\t' => ' ',
            c if c.is_control() => '\u{fffd}',
            c => c,
        })
        .collect();
    // Colorize only when stdout is a terminal, so redirected logs stay clean.
    if std::io::stdout().is_terminal() {
        println!("\x1b[1;34m[web-ui]\x1b[0m Executing: \x1b[36m{sql}\x1b[0m");
    } else {
        println!("[web-ui] Executing: {sql}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use tower::ServiceExt;

    // The API handlers need a full LakeletContext, but the CORS layer and the
    // fallback are independent of it, so test them on a stateless router.
    fn test_router() -> Router {
        Router::new().fallback(fallback).layer(cors_layer())
    }

    #[test]
    fn planning_errors_map_to_bad_request_through_wrappers() {
        // Table-not-found reaches the handler wrapped, so matching the outer
        // variant would misreport the most common user error as a 500.
        let wrapped = DataFusionError::Context(
            "planning".to_string(),
            Box::new(DataFusionError::Plan("table 'x' not found".to_string())),
        );
        assert_eq!(
            df_error_to_response(wrapped).status(),
            StatusCode::BAD_REQUEST
        );

        assert_eq!(
            df_error_to_response(DataFusionError::Execution("boom".to_string())).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn preflight_allows_any_origin_and_private_network() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/query")
                    .header("origin", "https://ui.example.com")
                    .header("access-control-request-method", "POST")
                    .header("access-control-request-headers", "content-type")
                    .header("access-control-request-private-network", "true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        assert_eq!(headers["access-control-allow-origin"], "*");
        assert_eq!(headers["access-control-allow-private-network"], "true");
    }

    #[tokio::test]
    async fn fallback_returns_not_found_json_with_cors() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("origin", "https://ui.example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "not_found");
    }
}
