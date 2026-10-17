mod metadata;
mod sql_info;

use crate::catalog::LakeletCatalogProviderList;
use crate::context::LakeletContext;
use crate::sql::session::ExtendedSessionContext;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
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
use std::pin::Pin;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};

pub struct LakeletFlightSqlService {
    // The process-wide list, shared across all per-request sessions so
    // catalog providers (and their metastore clients) are built once per
    // process, not per request.
    catalog_provider_list: Arc<LakeletCatalogProviderList>,
    lakelet_context: Arc<LakeletContext>,
    runtime_env: Arc<RuntimeEnv>,
}

impl LakeletFlightSqlService {
    pub fn new(
        catalog_provider_list: Arc<LakeletCatalogProviderList>,
        lakelet_context: Arc<LakeletContext>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Self {
        Self {
            catalog_provider_list,
            lakelet_context,
            runtime_env,
        }
    }

    // A fresh session per request: `create_dataframe` replaces the session's
    // catalog list with only the catalogs resolved for that query, so a shared
    // session would race under concurrent requests. Sharing the catalog
    // provider list is safe: its providers are registered at startup and the
    // map is immutable afterwards.
    fn new_session(&self, session_defaults: SessionDefaults) -> ExtendedSessionContext {
        let session = ExtendedSessionContext::new(
            self.catalog_provider_list.clone(),
            self.lakelet_context.clone(),
            self.runtime_env.clone(),
        );
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

    // Discover the result schema without executing persistent catalog DDL.
    async fn plan_schema(
        &self,
        sql: &str,
        session_defaults: SessionDefaults,
    ) -> Result<Schema, Status> {
        let schema = self
            .new_session(session_defaults)
            .sql_schema(sql)
            .await
            .map_err(df_error_to_status)?;
        Ok(advertised_schema(schema))
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
        let batch = builder.build().map_err(Status::from)?;
        Ok(metadata::single_batch_response(schema, batch))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_info = Self::flight_info(
            &metadata::catalogs_schema(),
            query.as_any().encode_to_vec(),
            request.into_inner(),
        )?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let batch = metadata::catalogs_batch(&self.catalog_provider_list)?;
        Ok(metadata::single_batch_response(
            metadata::catalogs_schema(),
            batch,
        ))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Reject an unsupported request here rather than at DoGet, so the
        // client never holds a ticket it cannot redeem.
        metadata::require_catalog(
            &self.catalog_provider_list,
            query.catalog.as_deref(),
            "GetDbSchemas",
        )?;
        let flight_info = Self::flight_info(
            &metadata::db_schemas_schema(),
            query.as_any().encode_to_vec(),
            request.into_inner(),
        )?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let batch = metadata::db_schemas_batch(&self.catalog_provider_list, &query).await?;
        Ok(metadata::single_batch_response(
            metadata::db_schemas_schema(),
            batch,
        ))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Reject an unsupported request here rather than at DoGet, so the
        // client never holds a ticket it cannot redeem. `include_schema` in
        // particular: the advertised schema has no table_schema column, so
        // letting it through would promise an answer to a different question.
        metadata::validate_tables_request(&self.catalog_provider_list, &query)?;
        let flight_info = Self::flight_info(
            &metadata::tables_schema(),
            query.as_any().encode_to_vec(),
            request.into_inner(),
        )?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let batch = metadata::tables_batch(&self.catalog_provider_list, &query).await?;
        Ok(metadata::single_batch_response(
            metadata::tables_schema(),
            batch,
        ))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_info = Self::flight_info(
            &metadata::table_types_schema(),
            query.as_any().encode_to_vec(),
            request.into_inner(),
        )?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let batch = metadata::table_types_batch()?;
        Ok(metadata::single_batch_response(
            metadata::table_types_schema(),
            batch,
        ))
    }

    // Sql-info is served from the static table in `sql_info`, so there is no
    // per-instance registry to fill.
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::paimon_fs::{PaimonFSCatalog, PaimonFSCatalogConfig};
    use crate::catalog::{
        INFORMATION_SCHEMA_SHOW_VARIABLES, INTERNAL_CATALOG, LakeletCatalogProvider,
    };
    use crate::server::tests::spawn_test_server;
    use arrow_flight::decode::FlightRecordBatchStream;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use datafusion::arrow::array::{Int64Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
    use lakelet_storage::storage::Storage;
    use std::collections::HashMap;
    use std::path::Path;
    use tonic::transport::Channel;

    async fn start_test_server() -> Result<FlightSqlServiceClient<Channel>> {
        let addr = spawn_test_server().await?;
        let channel = Channel::from_shared(format!("http://{addr}"))
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .connect()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(FlightSqlServiceClient::new(channel))
    }

    fn paimon_service(warehouse: &Path) -> LakeletFlightSqlService {
        let context = Arc::new(LakeletContext::default());
        let config = Arc::new(PaimonFSCatalogConfig {
            name: "paimon_fs".into(),
            warehouse: warehouse.to_str().unwrap().into(),
            storage: Storage::default(),
        });
        let catalog = Arc::new(PaimonFSCatalog::try_new(context.clone(), config).unwrap());
        LakeletFlightSqlService::new(
            Arc::new(LakeletCatalogProviderList::from_catalogs(HashMap::from([
                (
                    "paimon_fs".into(),
                    catalog as Arc<dyn LakeletCatalogProvider>,
                ),
            ]))),
            context,
            Arc::new(RuntimeEnv::default()),
        )
    }

    async fn ddl_ticket(service: &LakeletFlightSqlService, sql: &str) -> Ticket {
        let command = CommandStatementQuery {
            query: sql.into(),
            transaction_id: None,
        };
        let info = service
            .get_flight_info(Request::new(FlightDescriptor::new_cmd(
                command.as_any().encode_to_vec(),
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(info.clone().try_decode_schema().unwrap(), Schema::empty());
        assert_eq!(info.endpoint.len(), 1);
        info.endpoint[0].ticket.clone().unwrap()
    }

    #[tokio::test]
    async fn test_ddl_executes_only_on_do_get() {
        let warehouse = tempfile::TempDir::new().unwrap();
        let service = paimon_service(warehouse.path());

        // No IF [NOT] EXISTS: executing during both RPCs would make DoGet fail.
        for (sql, relative_path, exists_before) in [
            ("CREATE SCHEMA paimon_fs.db1", "db1.db", false),
            ("CREATE TABLE paimon_fs.db1.t1 (id INT)", "db1.db/t1", false),
            ("DROP TABLE paimon_fs.db1.t1", "db1.db/t1", true),
            ("DROP SCHEMA paimon_fs.db1", "db1.db", true),
        ] {
            let path = warehouse.path().join(relative_path);
            assert_eq!(path.exists(), exists_before, "{sql}");
            // Repeated schema discovery must not create or remove objects.
            let _ = ddl_ticket(&service, sql).await;
            assert_eq!(path.exists(), exists_before, "{sql}");
            let ticket = ddl_ticket(&service, sql).await;
            assert_eq!(path.exists(), exists_before, "{sql}");

            let response = service.do_get(Request::new(ticket)).await.unwrap();
            let mut stream = FlightRecordBatchStream::new_from_flight_data(
                response.into_inner().map_err(FlightError::from),
            );
            while let Some(batch) = stream.try_next().await.unwrap() {
                assert_eq!(batch.num_rows(), 0, "{sql}");
            }
            assert_eq!(stream.schema().unwrap().as_ref(), &Schema::empty());
            assert_eq!(path.exists(), !exists_before, "{sql}");
        }
    }

    #[tokio::test]
    async fn test_ddl_semantic_errors_are_deferred_to_do_get() {
        let warehouse = tempfile::TempDir::new().unwrap();
        let service = paimon_service(warehouse.path());
        let session = service.new_session(SessionDefaults::default());
        session.sql("CREATE SCHEMA paimon_fs.db1").await.unwrap();
        session
            .sql("CREATE TABLE paimon_fs.db1.t1 (id INT)")
            .await
            .unwrap();

        for (sql, expected_error) in [
            ("CREATE SCHEMA paimon_fs.db1", "already exists"),
            ("CREATE TABLE paimon_fs.db1.t1 (id INT)", "already exists"),
            ("CREATE SCHEMA missing.db1", "unknown catalog missing"),
            (
                "CREATE TABLE missing.db1.t1 (id INT)",
                "unknown catalog missing",
            ),
            ("DROP TABLE paimon_fs.db1.missing", "does not exist"),
            ("DROP SCHEMA paimon_fs.missing", "does not exist"),
            (
                "DROP TABLE paimon_fs.db1.t1 PURGE",
                "only supports DROP TABLE",
            ),
        ] {
            let ticket = ddl_ticket(&service, sql).await;
            // The stream is not Debug, so use err() instead of unwrap_err().
            let error = service.do_get(Request::new(ticket)).await.err().unwrap();
            assert!(error.message().contains(expected_error), "{sql}: {error}");
            assert!(warehouse.path().join("db1.db/t1/schema/schema-0").exists());
        }
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

        for sql in [
            "select from from",
            "",
            ";",
            "SELECT 1; SELECT 2",
            "CREATE SCHEMA",
            "CREATE SCHEMA missing.db1; DROP SCHEMA missing.db1",
        ] {
            let err = client
                .execute(sql.to_string(), None)
                .await
                .expect_err("invalid SQL should fail");
            assert!(
                matches!(err, FlightError::Tonic(ref status) if status.code() == tonic::Code::InvalidArgument),
                "{sql}: unexpected error: {err}"
            );
        }
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

    async fn fetch_batches(
        client: &mut FlightSqlServiceClient<Channel>,
        flight_info: FlightInfo,
    ) -> Vec<RecordBatch> {
        let ticket = flight_info.endpoint[0]
            .ticket
            .clone()
            .expect("endpoint should carry a ticket");
        client
            .do_get(ticket)
            .await
            .expect("do_get should stream results")
            .try_collect()
            .await
            .expect("result stream should decode")
    }

    fn string_column(batch: &RecordBatch, column_name: &str) -> Vec<String> {
        batch
            .column_by_name(column_name)
            .unwrap_or_else(|| panic!("batch should have a {column_name} column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column should be Utf8")
            .iter()
            .flatten()
            .map(str::to_string)
            .collect()
    }

    #[tokio::test]
    async fn test_get_metadata_endpoints() -> Result<()> {
        let mut client = start_test_server().await?;

        // The default context registers the internal catalog and nothing else,
        // so every listing below has exactly one known answer.
        let flight_info = client
            .get_catalogs()
            .await
            .expect("get_catalogs should succeed");
        let batches = fetch_batches(&mut client, flight_info).await;
        assert_eq!(
            string_column(&batches[0], "catalog_name"),
            vec![INTERNAL_CATALOG]
        );

        let flight_info = client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: Some(INTERNAL_CATALOG.to_string()),
                ..Default::default()
            })
            .await
            .expect("get_db_schemas should succeed");
        let batches = fetch_batches(&mut client, flight_info).await;
        assert_eq!(
            string_column(&batches[0], "db_schema_name"),
            vec![INFORMATION_SCHEMA]
        );

        let flight_info = client
            .get_tables(CommandGetTables {
                catalog: Some(INTERNAL_CATALOG.to_string()),
                db_schema_filter_pattern: Some(INFORMATION_SCHEMA.to_string()),
                ..Default::default()
            })
            .await
            .expect("get_tables should succeed");
        let batches = fetch_batches(&mut client, flight_info).await;
        assert_eq!(
            string_column(&batches[0], "table_name"),
            vec![INFORMATION_SCHEMA_SHOW_VARIABLES]
        );
        assert_eq!(string_column(&batches[0], "table_type"), vec!["TABLE"]);
        // The table_schema column is never advertised: include_schema is not
        // supported, so this is the one shape a GetTables result has.
        assert!(batches[0].column_by_name("table_schema").is_none());

        // Refused outright, narrow enough to be cheap or not — a table's
        // schema comes from GetFlightInfo on a statement instead.
        let err = client
            .get_tables(CommandGetTables {
                catalog: Some(INTERNAL_CATALOG.to_string()),
                db_schema_filter_pattern: Some(INFORMATION_SCHEMA.to_string()),
                table_name_filter_pattern: Some(INFORMATION_SCHEMA_SHOW_VARIABLES.to_string()),
                include_schema: true,
                ..Default::default()
            })
            .await
            .expect_err("include_schema should be refused");
        assert!(
            matches!(err, FlightError::Tonic(ref status) if status.code() == tonic::Code::Unimplemented),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("include_schema"),
            "unexpected error: {err}"
        );

        let flight_info = client
            .get_table_types()
            .await
            .expect("get_table_types should succeed");
        let batches = fetch_batches(&mut client, flight_info).await;
        assert_eq!(string_column(&batches[0], "table_type"), vec!["TABLE"]);
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
