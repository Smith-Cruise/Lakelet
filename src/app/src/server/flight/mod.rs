use crate::context::DobbyDbContext;
use crate::sql::session::ExtendedSessionContext;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
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
    dobbydb_context: Arc<DobbyDbContext>,
    runtime_env: Arc<RuntimeEnv>,
    port: u16,
) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| DataFusionError::Configuration(format!("Failed to bind {addr}: {e}")))?;
    let service = DobbyDbFlightSqlService::new(dobbydb_context, runtime_env);
    println!("DobbyDB Flight SQL server listening on {addr}");
    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub struct DobbyDbFlightSqlService {
    dobbydb_context: Arc<DobbyDbContext>,
    runtime_env: Arc<RuntimeEnv>,
}

impl DobbyDbFlightSqlService {
    pub fn new(dobbydb_context: Arc<DobbyDbContext>, runtime_env: Arc<RuntimeEnv>) -> Self {
        Self {
            dobbydb_context,
            runtime_env,
        }
    }

    // A fresh session per request: `create_dataframe` replaces the session's
    // catalog list with only the catalogs resolved for that query, so a shared
    // session would race under concurrent requests.
    fn new_session(&self) -> ExtendedSessionContext {
        ExtendedSessionContext::new(self.dobbydb_context.clone(), self.runtime_env.clone())
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
impl FlightSqlService for DobbyDbFlightSqlService {
    type FlightService = DobbyDbFlightSqlService;

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
        // Plan only (no execution) to learn the result schema. The SQL text is
        // embedded in the ticket, so the server stays stateless; DoGet re-plans.
        let dataframe = self
            .new_session()
            .sql(&query.query)
            .await
            .map_err(df_error_to_status)?;
        let schema = dataframe.schema().as_arrow().clone();

        let ticket = TicketStatementQuery {
            statement_handle: query.query.into_bytes().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());
        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid statement handle: {e}")))?;
        log_executing(&sql);
        let dataframe = self
            .new_session()
            .sql(&sql)
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

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use datafusion::arrow::array::Int64Array;
    use tonic::transport::Channel;

    async fn start_test_server() -> Result<FlightSqlServiceClient<Channel>> {
        let dobbydb_context = Arc::new(DobbyDbContext::default());
        let runtime_env = Arc::new(RuntimeEnv::default());
        let service = DobbyDbFlightSqlService::new(dobbydb_context, runtime_env);

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
