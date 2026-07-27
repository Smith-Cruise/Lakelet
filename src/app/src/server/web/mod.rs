//! HTTP server behind `lakelet --ui`.
//!
//! One listener on `web-ui-port` serves both the REST API documented below
//! and the web UI: any path that is not an API route is served by
//! reverse-proxying the hosted static site (see [`proxy`]). The browser
//! talks to one origin for both the UI and the API, so no CORS is involved.
//!
//! # REST API
//!
//! Base URL: `http://<host>:<web-ui-port>` (no authentication).
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
//! Proxied to the hosted UI site (which answers unknown paths with the SPA's
//! `index.html`).
//!
//! # Security
//!
//! There is no CORS allowance: browsers only reach the API same-origin.
//! The server has no authentication, so any host that can reach the port
//! can query the configured catalogs. Only expose it on networks you trust.

mod proxy;

use crate::context::LakeletContext;
use crate::sql::session::ExtendedSessionContext;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::handler::Handler;
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
use std::net::SocketAddr;
use std::sync::Arc;

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
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| super::bind_error(port, "web-ui-port", &e))?;
    let app = router(AppState {
        lakelet_context,
        runtime_env,
    });
    print_instructions(port);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

// 127.0.0.1, not `localhost`: the server binds IPv4 only, while `localhost`
// may resolve to ::1 in some clients. The API is an internal detail of the
// UI, so the instructions mention only the UI address.
fn print_instructions(port: u16) {
    println!("Lakelet is running:");
    println!("  Web UI: http://127.0.0.1:{port}");
    if let Some(upstream) = proxy::ProxyState::upstream_override() {
        println!(
            "  UI upstream override ({}): {upstream}",
            proxy::UI_UPSTREAM_ENV
        );
    }
    println!("Press Ctrl+C to stop.");
}

// API routes win over the fallback, so everything that is not the API is
// served by proxying the hosted UI site — one origin for both.
fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/query", post(query))
        .route("/api/info", get(info))
        .with_state(state)
        .fallback_service(proxy::proxy_ui.with_state(proxy::ProxyState::from_env()))
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
        println!("\x1b[1;34m[ui]\x1b[0m Executing: \x1b[36m{sql}\x1b[0m");
    } else {
        println!("[ui] Executing: {sql}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn multi_statement_requests_are_client_errors() {
        // The editor can easily hold two statements; that is bad input, not an
        // internal failure.
        let session = ExtendedSessionContext::default();
        let err = session
            .sql("select 1; select 2")
            .await
            .expect_err("multiple statements should be rejected");
        assert_eq!(
            df_error_to_response(err).status(),
            StatusCode::BAD_REQUEST,
            "statement-count validation must map to invalid_sql"
        );
    }
}
