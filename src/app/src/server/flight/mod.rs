mod sql_info;

use crate::context::LakeletContext;
use crate::sql::session::ExtendedSessionContext;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetSqlInfo, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, DoPutPreparedStatementResult,
    ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::{Stream, TryStreamExt};
use prost::Message;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
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
    fn new_session(&self) -> ExtendedSessionContext {
        ExtendedSessionContext::new(self.lakelet_context.clone(), self.runtime_env.clone())
    }

    // Plan only (no execution) to learn the result schema.
    async fn plan_schema(&self, sql: &str) -> Result<Schema, Status> {
        let dataframe = self
            .new_session()
            .sql(sql)
            .await
            .map_err(df_error_to_status)?;
        Ok(dataframe.schema().as_arrow().clone())
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
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log_executing(sql);
        let dataframe = self
            .new_session()
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

// The handle is the SQL text itself: the server stays stateless, at the cost
// of planning a prepared query once per RPC (prepare, GetFlightInfo, DoGet).
fn handle_to_sql(handle: &[u8]) -> Result<String, Status> {
    String::from_utf8(handle.to_vec())
        .map_err(|e| Status::invalid_argument(format!("Invalid statement handle: {e}")))
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
        let schema = self.plan_schema(&query.query).await?;
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
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = handle_to_sql(&ticket.statement_handle)?;
        self.execute_sql(&sql).await
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = handle_to_sql(&query.prepared_statement_handle)?;
        let schema = self.plan_schema(&sql).await?;
        // Round-trip the command in the ticket so DoGet dispatches back to
        // do_get_prepared_statement.
        let flight_info = Self::flight_info(
            &schema,
            query.as_any().encode_to_vec(),
            request.into_inner(),
        )?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = handle_to_sql(&query.prepared_statement_handle)?;
        self.execute_sql(&sql).await
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

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        // Clients only issue this DoPut to bind parameters, but the stream may
        // legitimately hold a schema-only message or zero-row batches; reject
        // only actual parameter rows.
        let flight_data: Vec<FlightData> = request.into_inner().try_collect().await?;
        let batches: Vec<_> = FlightRecordBatchStream::new_from_flight_data(futures::stream::iter(
            flight_data.into_iter().map(Ok::<_, FlightError>),
        ))
        .try_collect()
        .await
        .map_err(|e| Status::invalid_argument(format!("Invalid parameter stream: {e}")))?;
        if batches.iter().any(|batch| batch.num_rows() > 0) {
            return Err(Status::invalid_argument(
                "Lakelet does not support prepared statement parameters",
            ));
        }
        // Echo the handle back unchanged so the client keeps using it.
        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(query.prepared_statement_handle),
        })
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "Lakelet is a read-only query engine; update statements are not supported",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        // Plan once to validate the SQL and learn the result schema. The
        // handle is the SQL text itself, so no server-side state is created.
        let schema = self.plan_schema(&query.query).await?;
        let IpcMessage(schema_bytes) = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?;
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: query.query.into_bytes().into(),
            dataset_schema: schema_bytes,
            // Empty bytes: parameters are not supported, clients decode this
            // as an empty parameter schema.
            parameter_schema: Default::default(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        // Nothing to release: prepared statements hold no server-side state.
        Ok(())
    }

    // Sql-info is served from the static table in `sql_info`, so there is no
    // per-instance registry to fill.
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, UInt32Array};
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
    async fn test_prepared_statement_roundtrip() -> Result<()> {
        let mut client = start_test_server().await?;

        let mut stmt = client
            .prepare("select 1 as a".to_string(), None)
            .await
            .expect("prepare should succeed");
        let dataset_schema = stmt
            .dataset_schema()
            .expect("prepare should return the result schema");
        assert_eq!(dataset_schema.field(0).name(), "a");
        let parameter_schema = stmt
            .parameter_schema()
            .expect("prepare should return a parameter schema");
        assert!(parameter_schema.fields().is_empty());

        let flight_info = stmt.execute().await.expect("execute should succeed");
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

        stmt.close().await.expect("close should succeed");
        Ok(())
    }

    #[tokio::test]
    async fn test_prepare_invalid_sql() -> Result<()> {
        let mut client = start_test_server().await?;

        let err = client
            .prepare("select from from".to_string(), None)
            .await
            .expect_err("preparing invalid SQL should fail");
        assert!(
            err.to_string().contains("invalid argument"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_prepared_statement_parameters_rejected() -> Result<()> {
        let mut client = start_test_server().await?;

        let mut stmt = client
            .prepare("select 1 as a".to_string(), None)
            .await
            .expect("prepare should succeed");
        let params = RecordBatch::try_from_iter(vec![(
            "p",
            Arc::new(Int64Array::from(vec![42])) as ArrayRef,
        )])
        .expect("parameter batch should build");
        stmt.set_parameters(params)
            .expect("setting parameters client-side should succeed");
        let err = stmt
            .execute()
            .await
            .expect_err("executing with parameters should fail");
        assert!(
            err.to_string().contains("parameters"),
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
