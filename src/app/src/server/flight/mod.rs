mod sql_info;

use crate::context::LakeletContext;
use crate::sql::session::ExtendedSessionContext;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetSqlInfo, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::{Stream, TryStreamExt};
use prost::Message;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status, Streaming};

pub async fn serve(
    lakelet_context: Arc<LakeletContext>,
    runtime_env: Arc<RuntimeEnv>,
    port: u16,
) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| super::bind_error(port, "flight-sql-server-port", &e))?;
    let service = LakeletFlightSqlService::new(lakelet_context, runtime_env);
    println!("Lakelet Flight SQL server listening on port {port}");
    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub struct LakeletFlightSqlService {
    lakelet_context: Arc<LakeletContext>,
    runtime_env: Arc<RuntimeEnv>,
}

impl LakeletFlightSqlService {
    pub fn new(lakelet_context: Arc<LakeletContext>, runtime_env: Arc<RuntimeEnv>) -> Self {
        Self {
            lakelet_context,
            runtime_env,
        }
    }

    // A fresh session per request: `create_dataframe` replaces the session's
    // catalog list with only the catalogs resolved for that query, so a shared
    // session would race under concurrent requests.
    fn new_session(&self, session_defaults: SessionDefaults) -> ExtendedSessionContext {
        let session =
            ExtendedSessionContext::new(self.lakelet_context.clone(), self.runtime_env.clone());
        let state = session.session_context().state_ref();
        let mut state = state.write();
        let config_options = state.config_mut().options_mut();
        if let Some(default_catalog) = session_defaults.default_catalog {
            config_options.catalog.default_catalog = default_catalog;
        }
        if let Some(default_schema) = session_defaults.default_schema {
            config_options.catalog.default_schema = default_schema;
        }
        session
    }

    // Plan only (no execution) to learn the result schema.
    async fn plan_schema(
        &self,
        sql: &str,
        session_defaults: SessionDefaults,
    ) -> Result<Schema, Status> {
        let dataframe = self
            .new_session(session_defaults)
            .sql(sql)
            .await
            .map_err(df_error_to_status)?;
        Ok(advertised_schema(dataframe.schema().as_arrow().clone()))
    }

    // Build a single-endpoint FlightInfo whose ticket carries the given
    // Any-encoded command, so DoGet dispatches back to the matching handler.
    fn flight_info(
        schema: &Schema,
        ticket: Vec<u8>,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.into(),
        });
        Ok(FlightInfo::new()
            .try_with_schema(schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor))
    }

    // Re-plan and execute, streaming the result batches back to the client.
    async fn execute_sql(
        &self,
        sql: &str,
        session_defaults: SessionDefaults,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log_executing(sql);
        let dataframe = self
            .new_session(session_defaults)
            .sql(sql)
            .await
            .map_err(df_error_to_status)?;
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let batch_stream = dataframe
            .execute_stream()
            .await
            .map_err(df_error_to_status)?
            .map_err(|e| FlightError::ExternalError(Box::new(e)));
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);
        Ok(Response::new(Box::pin(flight_data_stream)))
    }
}

// The schema the DoGet stream actually carries: FlightDataEncoder hydrates
// dictionary columns (e.g. Delta partition columns) to their value types, so
// run the planned schema through the same preparation. Advertising the raw
// planned schema makes strict clients (the ADBC flightsql driver) reject the
// stream as inconsistent.
fn advertised_schema(schema: Schema) -> Schema {
    let encoder = FlightDataEncoderBuilder::new()
        .with_schema(Arc::new(schema))
        .build(futures::stream::empty());
    let schema = encoder
        .known_schema()
        .expect("with_schema always sets the encoder schema");
    schema.as_ref().clone()
}

// The statement ticket handle is the SQL text itself, so the server stays
// stateless. DoGet uses it to reconstruct and execute the query.
fn handle_to_sql(handle: &[u8]) -> Result<String, Status> {
    String::from_utf8(handle.to_vec())
        .map_err(|e| Status::invalid_argument(format!("Invalid statement handle: {e}")))
}

/// Per-request default catalog/schema, from the `default-catalog` and
/// `default-schema` gRPC metadata headers — named after the CLI flags of the
/// same spelling. Every RPC reads its own headers (nothing is baked into
/// statement tickets), so clients must send them on each call — which ADBC's
/// connection-level `adbc.flight.sql.rpc.call_header.*` options already do.
#[derive(Default)]
struct SessionDefaults {
    default_catalog: Option<String>,
    default_schema: Option<String>,
}

impl SessionDefaults {
    fn from_metadata(metadata: &MetadataMap) -> Result<Self, Status> {
        let extract_from_header = |key: &str| {
            metadata
                .get(key)
                .map(|value| {
                    value.to_str().map(str::to_string).map_err(|_| {
                        Status::invalid_argument(format!(
                            "Invalid '{key}' header: value must be visible ASCII"
                        ))
                    })
                })
                .transpose()
        };
        Ok(Self {
            default_catalog: extract_from_header("default-catalog")?,
            default_schema: extract_from_header("default-schema")?,
        })
    }
}

fn log_executing(sql: &str) {
    // Colorize only when stdout is a terminal, so redirected logs stay clean.
    if std::io::stdout().is_terminal() {
        println!("\x1b[1;32m[flight-sql-server]\x1b[0m Executing: \x1b[36m{sql}\x1b[0m");
    } else {
        println!("[flight-sql-server] Executing: {sql}");
    }
}

fn df_error_to_status(err: DataFusionError) -> Status {
    match err {
        DataFusionError::Plan(_)
        | DataFusionError::SQL(..)
        | DataFusionError::SchemaError(..)
        | DataFusionError::Configuration(_) => Status::invalid_argument(err.to_string()),
        DataFusionError::NotImplemented(_) => Status::unimplemented(err.to_string()),
        DataFusionError::ResourcesExhausted(_) => Status::resource_exhausted(err.to_string()),
        _ => Status::internal(err.to_string()),
    }
}

#[tonic::async_trait]
impl FlightSqlService for LakeletFlightSqlService {
    type FlightService = LakeletFlightSqlService;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        // No authentication: accept every handshake with a single empty response.
        let stream = futures::stream::iter([Ok(HandshakeResponse::default())]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // The SQL text is embedded in the ticket, so the server stays
        // stateless; DoGet re-plans.
        let session_defaults = SessionDefaults::from_metadata(request.metadata())?;
        let schema = self.plan_schema(&query.query, session_defaults).await?;
        let ticket = TicketStatementQuery {
            statement_handle: query.query.into_bytes().into(),
        };
        let flight_info = Self::flight_info(
            &schema,
            ticket.as_any().encode_to_vec(),
            request.into_inner(),
        )?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session_defaults = SessionDefaults::from_metadata(request.metadata())?;
        let sql = handle_to_sql(&ticket.statement_handle)?;
        self.execute_sql(&sql, session_defaults).await
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Encode the ticket before `into_builder` consumes the command, so
        // DoGet receives the same info filter list.
        let ticket = query.as_any().encode_to_vec();
        let builder = query.into_builder(&sql_info::SQL_INFO_DATA);
        let flight_info = Self::flight_info(&builder.schema(), ticket, request.into_inner())?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let builder = query.into_builder(&sql_info::SQL_INFO_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    // Sql-info is served from the static table in `sql_info`, so there is no
    // per-instance registry to fill.
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use datafusion::arrow::array::{Int64Array, UInt32Array};
    use datafusion::arrow::datatypes::DataType;
    use tonic::transport::Channel;

    async fn start_test_server() -> Result<FlightSqlServiceClient<Channel>> {
        let lakelet_context = Arc::new(LakeletContext::default());
        let runtime_env = Arc::new(RuntimeEnv::default());
        let service = LakeletFlightSqlService::new(lakelet_context, runtime_env);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming(TcpIncoming::from(listener)),
        );

        let channel = Channel::from_shared(format!("http://{addr}"))
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .connect()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(FlightSqlServiceClient::new(channel))
    }

    #[tokio::test]
    async fn test_execute_statement_query() -> Result<()> {
        let mut client = start_test_server().await?;

        let flight_info = client
            .execute("select 1 as a".to_string(), None)
            .await
            .expect("execute should return a FlightInfo");
        assert_eq!(flight_info.endpoint.len(), 1);
        let schema = flight_info
            .clone()
            .try_decode_schema()
            .expect("FlightInfo should carry the result schema");
        assert_eq!(schema.field(0).name(), "a");

        let ticket = flight_info.endpoint[0]
            .ticket
            .clone()
            .expect("endpoint should carry a ticket");
        let batches: Vec<_> = client
            .do_get(ticket)
            .await
            .expect("do_get should stream results")
            .try_collect()
            .await
            .expect("result stream should decode");

        assert_eq!(batches.len(), 1);
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column should be Int64");
        assert_eq!(column.value(0), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_invalid_argument() -> Result<()> {
        let mut client = start_test_server().await?;

        let err = client
            .execute("select from from".to_string(), None)
            .await
            .expect_err("invalid SQL should fail");
        assert!(
            err.to_string().contains("invalid argument"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_prepared_statement_is_unimplemented() -> Result<()> {
        let mut client = start_test_server().await?;

        let err = client
            .prepare("select 1 as a".to_string(), None)
            .await
            .expect_err("prepare should be unsupported");
        assert!(
            matches!(err, FlightError::Tonic(ref status) if status.code() == tonic::Code::Unimplemented),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_sql_info() -> Result<()> {
        let mut client = start_test_server().await?;

        // An empty filter returns every registered info entry.
        let flight_info = client
            .get_sql_info(vec![])
            .await
            .expect("get_sql_info should succeed");
        let ticket = flight_info.endpoint[0]
            .ticket
            .clone()
            .expect("endpoint should carry a ticket");
        let batches: Vec<_> = client
            .do_get(ticket)
            .await
            .expect("do_get should stream results")
            .try_collect()
            .await
            .expect("result stream should decode");
        assert_eq!(batches.len(), 1);
        assert!(batches[0].num_rows() >= 6, "expected all sql info entries");

        // A filtered request returns only the requested entry.
        let flight_info = client
            .get_sql_info(vec![SqlInfo::FlightSqlServerName])
            .await
            .expect("filtered get_sql_info should succeed");
        let ticket = flight_info.endpoint[0]
            .ticket
            .clone()
            .expect("endpoint should carry a ticket");
        let batches: Vec<_> = client
            .do_get(ticket)
            .await
            .expect("do_get should stream results")
            .try_collect()
            .await
            .expect("result stream should decode");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let info_name = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("info_name should be UInt32");
        assert_eq!(info_name.value(0), SqlInfo::FlightSqlServerName as u32);
        Ok(())
    }

    #[tokio::test]
    async fn test_catalog_schema_headers_applied() -> Result<()> {
        let mut client = start_test_server().await?;

        // Baseline: the server defaults (internal.information_schema) resolve
        // the unqualified name.
        client
            .execute("select * from variables".to_string(), None)
            .await
            .expect("default catalog/schema should resolve 'variables'");

        // A bogus schema header overrides the default and breaks resolution
        // at the GetFlightInfo (planning) stage.
        client.set_header("default-schema", "no_such_schema");
        client
            .execute("select * from variables".to_string(), None)
            .await
            .expect_err("bogus 'schema' header should break name resolution");

        // Explicit valid headers work across the whole GetFlightInfo -> DoGet
        // chain: set_header attaches them to every call from this client.
        client.set_header("default-catalog", "internal");
        client.set_header("default-schema", "information_schema");
        let flight_info = client
            .execute("select * from variables".to_string(), None)
            .await
            .expect("valid catalog/schema headers should resolve 'variables'");
        let ticket = flight_info.endpoint[0]
            .ticket
            .clone()
            .expect("endpoint should carry a ticket");
        let _batches: Vec<_> = client
            .do_get(ticket)
            .await
            .expect("do_get should honor the same headers")
            .try_collect()
            .await
            .expect("result stream should decode");
        Ok(())
    }

    #[test]
    fn test_session_defaults_from_metadata() {
        let mut metadata = MetadataMap::new();
        let session_defaults =
            SessionDefaults::from_metadata(&metadata).expect("empty metadata should parse");
        assert_eq!(session_defaults.default_catalog, None);
        assert_eq!(session_defaults.default_schema, None);

        metadata.insert("default-catalog", "hive".parse().unwrap());
        metadata.insert("default-schema", "sales".parse().unwrap());
        let session_defaults =
            SessionDefaults::from_metadata(&metadata).expect("valid headers should parse");
        assert_eq!(session_defaults.default_catalog.as_deref(), Some("hive"));
        assert_eq!(session_defaults.default_schema.as_deref(), Some("sales"));
    }

    #[tokio::test]
    async fn test_dictionary_schema_advertised_hydrated() -> Result<()> {
        let mut client = start_test_server().await?;

        // Dictionary columns (e.g. Delta partition columns) are hydrated by
        // the DoGet encoder; the advertised schema must match or strict
        // clients (the ADBC flightsql driver) reject the stream.
        let sql = "select arrow_cast('a', 'Dictionary(Int32, Utf8)') as d".to_string();

        let flight_info = client
            .execute(sql, None)
            .await
            .expect("execute should return a FlightInfo");
        let schema = flight_info
            .clone()
            .try_decode_schema()
            .expect("FlightInfo should carry the result schema");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);

        let ticket = flight_info.endpoint[0]
            .ticket
            .clone()
            .expect("endpoint should carry a ticket");
        let batches: Vec<_> = client
            .do_get(ticket)
            .await
            .expect("do_get should stream results")
            .try_collect()
            .await
            .expect("result stream should decode");
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Utf8);
        Ok(())
    }

    #[test]
    fn test_df_error_to_status_mapping() {
        let status = df_error_to_status(DataFusionError::Plan("bad plan".to_string()));
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let status = df_error_to_status(DataFusionError::NotImplemented("nope".to_string()));
        assert_eq!(status.code(), tonic::Code::Unimplemented);

        let status = df_error_to_status(DataFusionError::Execution("boom".to_string()));
        assert_eq!(status.code(), tonic::Code::Internal);
    }
}
