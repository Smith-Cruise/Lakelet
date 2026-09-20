pub mod cli_helper;
pub mod flight;
pub mod print;
pub mod repl;
pub mod web;

use crate::catalog::LakeletCatalogProviderList;
use crate::context::LakeletContext;
use crate::server::flight::LakeletFlightSqlService;
use crate::server::print::PrintOptions;
use crate::sql::session::ExtendedSessionContext;
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use datafusion::common::error::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::DefaultObjectStoreRegistry;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::MaxRows;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use tonic::transport::server::{Router, TcpIncoming};

/// Turns a TCP bind failure into a configuration error. When the port is
/// already taken, points the user at the config key that controls it.
fn bind_error(port: u16, config_key: &str, e: &std::io::Error) -> DataFusionError {
    let mut message = format!("Failed to bind port {port}: {e}");
    if e.kind() == std::io::ErrorKind::AddrInUse {
        message.push_str(&format!(
            "; set a different '{config_key}' under [server] in the config file"
        ));
    }
    DataFusionError::Configuration(message)
}

/// Binds the one port both protocols share and serves them until Ctrl-C.
pub async fn serve(
    catalog_provider_list: Arc<LakeletCatalogProviderList>,
    lakelet_context: Arc<LakeletContext>,
    runtime_env: Arc<RuntimeEnv>,
    port: u16,
) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| bind_error(port, "server-port", &e))?;
    let service = LakeletFlightSqlService::new(catalog_provider_list, lakelet_context, runtime_env);
    println!("Lakelet server is running");
    println!("  Flight SQL  grpc://localhost:{port}");
    println!("  Web UI      {}", web::status_line(port));
    router(service)
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown_signal())
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Routes native Flight SQL, gRPC-Web and the web UI on the one port.
///
/// `accept_http1` swaps hyper's HTTP/2-only connection builder for the one that
/// sniffs the HTTP/2 client preface, so a native client speaking h2c with prior
/// knowledge still lands on HTTP/2 exactly as before; only connections that are
/// not HTTP/2 fall through to HTTP/1, which is what a browser sends.
///
/// gRPC-Web wraps the Flight service alone rather than the whole server:
/// `GrpcWebLayer` answers any HTTP/1.1 request that is not gRPC-Web with 400,
/// so installing it globally would reject every request for the UI itself.
/// `Routes` must be built from the UI router, because a `Routes` created from a
/// service falls back to a gRPC `UNIMPLEMENTED` response and would leave the
/// UI unreachable.
fn router(service: LakeletFlightSqlService) -> Router {
    use tonic::service::{LayerExt, Routes};

    let routes = Routes::from(web::router())
        .add_service(tonic_web::GrpcWebLayer::new().named_layer(FlightServiceServer::new(service)));
    Server::builder().accept_http1(true).add_routes(routes)
}

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct LakeletArgs {
    #[clap(
        long,
        help = "Specify config path",
        required_unless_present = "version"
    )]
    config: Option<String>,

    #[clap(long, help = "Specify the default catalog name")]
    default_catalog: Option<String>,

    #[clap(long, help = "Specify the default schema name")]
    default_schema: Option<String>,

    #[clap(
        long,
        help = "Execute the given command string, then exit. The command is expected to be non empty. Conflicts with --file.",
        value_parser(parse_command),
        conflicts_with = "file"
    )]
    command: Option<String>,

    #[clap(
        long,
        help = "Execute commands from a file, then exit. The file is expected to exist. Conflicts with --command.",
        value_parser(parse_file),
        conflicts_with = "command"
    )]
    file: Option<String>,

    #[clap(
        long,
        help = "Start the Lakelet server (Arrow Flight SQL plus the web UI) instead of the interactive REPL. Listens on 'server-port' under [server] in the config file (default 32010). Conflicts with --command and --file.",
        conflicts_with_all = ["command", "file"]
    )]
    server: bool,

    #[clap(
        short = 'V',
        long,
        help = "Print the commit this binary was built from, then exit."
    )]
    version: bool,
}

pub fn run() -> Result<()> {
    let args = LakeletArgs::parse();
    if args.version {
        println!("{}", crate::version::BUILD_INFO);
        return Ok(());
    }
    let mut lakelet_context = LakeletContext::new(args.config.as_deref())?;
    lakelet_context.default_catalog = args.default_catalog.clone();
    lakelet_context.default_schema = args.default_schema.clone();
    let lakelet_context = Arc::new(lakelet_context);
    let cpu_handle = lakelet_context.runtime_manager.cpu_handle();
    // Keep an owner outside the async context so RuntimeManager is dropped
    // after block_on returns, not from within its own Tokio runtime.
    cpu_handle.block_on(async_run(lakelet_context.clone(), args))
}

async fn async_run(lakelet_context: Arc<LakeletContext>, args: LakeletArgs) -> Result<()> {
    let memory_limit = lakelet_context.server_config.resolve_memory_limit()?;
    let runtime_env_builder = RuntimeEnvBuilder::new().with_memory_limit(memory_limit, 1.0);
    let runtime_env = runtime_env_builder
        .with_metadata_cache_limit(128 * 1024 * 1024) // 128MB parquet metadata cache
        .with_object_list_cache_limit(5 * 1024 * 1024) // 5MB
        .with_object_list_cache_ttl(Some(Duration::from_hours(1))) // 1 hour cache
        .with_object_store_registry(Arc::new(DefaultObjectStoreRegistry::new()))
        .build_arc()?;

    let catalog_provider_list = Arc::new(LakeletCatalogProviderList::new(lakelet_context.clone())?);

    if args.server {
        let port = lakelet_context.server_config.server_port;
        return serve(catalog_provider_list, lakelet_context, runtime_env, port).await;
    }

    let print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: false,
        maxrows: MaxRows::Unlimited,
    };
    let session_context =
        ExtendedSessionContext::new(catalog_provider_list, lakelet_context, runtime_env);
    let command = args.command;
    let file = args.file;
    if let Some(command) = command {
        repl::exec_from_commands(&session_context, &command, &print_options).await?;
    } else if let Some(file) = file {
        repl::exec_from_file(&session_context, &file, &print_options).await?;
    } else {
        repl::exec_from_repl(&session_context, &print_options).await;
    }
    Ok(())
}

fn parse_command(command: &str) -> Result<String, String> {
    if !command.is_empty() {
        Ok(command.to_string())
    } else {
        Err("-c flag expects only non empty commands".to_string())
    }
}

fn parse_file(file: &str) -> Result<String, String> {
    if file.is_empty() {
        return Err("--file expects a non empty file path".to_string());
    }

    let path = Path::new(file);
    if path.is_file() {
        Ok(file.to_string())
    } else {
        Err(format!("--file expects an existing file path, got: {file}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::fs;
    use tempfile::NamedTempFile;

    /// Serves the production router on an ephemeral port, so every test
    /// goes through the same wiring as `serve`, web UI and gRPC-Web layers
    /// included.
    pub(super) async fn spawn_test_server() -> Result<SocketAddr> {
        let lakelet_context = Arc::new(LakeletContext::default());
        let runtime_env = Arc::new(RuntimeEnv::default());
        let catalog_provider_list =
            Arc::new(LakeletCatalogProviderList::new(lakelet_context.clone())?);
        let service =
            LakeletFlightSqlService::new(catalog_provider_list, lakelet_context, runtime_env);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(router(service).serve_with_incoming(TcpIncoming::from(listener)));
        Ok(addr)
    }

    /// A browser speaks HTTP/1.1 to the same port as Flight SQL: unknown
    /// paths reach the UI router (not a gRPC error), and `/` serves the
    /// bundle when one is built.
    #[tokio::test]
    async fn test_http1_requests_reach_web_ui() -> Result<()> {
        let addr = spawn_test_server().await?;

        let response = http1_get(addr, "/no-such-path").await?;
        assert!(
            response.starts_with("HTTP/1.1 404 "),
            "unexpected response: {response}"
        );

        let response = http1_get(addr, "/").await?;
        if web::IS_BUNDLED {
            assert!(
                response.starts_with("HTTP/1.1 200 "),
                "unexpected response: {response}"
            );
            assert!(response.contains("<div id=\"root\">"));
        } else {
            assert!(
                response.starts_with("HTTP/1.1 404 "),
                "unexpected response: {response}"
            );
            assert!(response.contains(web::MISSING_BUNDLE));
        }
        Ok(())
    }

    /// Fetches `path` with a bare HTTP/1.1 request and returns the raw
    /// response, headers and body included.
    async fn http1_get(addr: SocketAddr, path: &str) -> Result<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok(String::from_utf8_lossy(&response).into_owned())
    }

    #[test]
    fn test_parse_single_command() {
        let args = LakeletArgs::try_parse_from([
            "lakelet",
            "--config",
            "config.toml",
            "--command",
            "show catalogs; show variables;",
        ])
        .expect("single command should parse");

        assert_eq!(
            args.command.as_deref(),
            Some("show catalogs; show variables;")
        );
        assert_eq!(args.config.as_deref(), Some("config.toml"));
    }

    #[test]
    fn test_parse_single_file() {
        let file = NamedTempFile::new().expect("temp sql file should be created");
        fs::write(file.path(), "show catalogs;show variables;")
            .expect("temp sql file should be written");
        let args = LakeletArgs::try_parse_from([
            "lakelet",
            "--config",
            "config.toml",
            "--file",
            file.path()
                .to_str()
                .expect("temp sql file path should be valid utf-8"),
        ])
        .expect("single file should parse");

        let file_path = file
            .path()
            .to_str()
            .expect("temp sql file path should be valid utf-8");
        assert_eq!(args.file.as_deref(), Some(file_path));
    }

    #[test]
    fn test_parse_requires_config() {
        let err = LakeletArgs::try_parse_from(["lakelet", "--command", "show catalogs;"])
            .expect_err("normal execution should require --config");

        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn test_parse_server() {
        let args = LakeletArgs::try_parse_from(["lakelet", "--config", "config.toml", "--server"])
            .expect("--server should parse");

        assert!(args.server);
    }

    #[test]
    fn test_parse_server_conflicts_with_command_and_file() {
        let file = NamedTempFile::new().expect("temp sql file should be created");
        let file_path = file
            .path()
            .to_str()
            .expect("temp sql file path should be valid utf-8");

        for conflicting in [
            vec!["--command", "show catalogs;"],
            vec!["--file", file_path],
        ] {
            let mut argv = vec!["lakelet", "--config", "config.toml", "--server"];
            argv.extend(conflicting);
            let err = LakeletArgs::try_parse_from(argv)
                .expect_err("--server should conflict with --command/--file");

            assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
        }
    }
}
