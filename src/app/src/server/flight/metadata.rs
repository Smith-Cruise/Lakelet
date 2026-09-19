use super::{LakeletFlightSqlService, df_error_to_status};
use crate::catalog::{LakeletCatalogProvider, LakeletCatalogProviderList};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::metadata::{GetCatalogsBuilder, GetDbSchemasBuilder, GetTablesBuilder};
use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::compute::kernels::comparison::like;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use std::sync::Arc;
use tonic::{Response, Status};

/// The only table type Lakelet can report. HMS `get_all_tables` and Glue's
/// table list both drop the upstream type, so nothing below the catalog layer
/// distinguishes a view from a table today.
const TABLE_TYPE: &str = "TABLE";

/// Everything `tables_batch` needs from a `CommandGetTables`, resolved once so
/// GetFlightInfo and DoGet reject exactly the same requests.
pub(super) struct TablesRequest {
    catalog_name: String,
    catalog: Arc<dyn LakeletCatalogProvider>,
    db_schema_filter_pattern: String,
    table_name_filter_pattern: Option<String>,
    include_schema: bool,
}

// The result schemas are constants, so GetFlightInfo can advertise them
// without contacting a single metastore; the listing itself waits for DoGet.

pub(super) fn catalogs_schema() -> SchemaRef {
    GetCatalogsBuilder::new().schema()
}

pub(super) fn db_schemas_schema() -> SchemaRef {
    GetDbSchemasBuilder::new(None::<String>, None::<String>).schema()
}

/// With `include_schema` the result carries an extra `table_schema` column,
/// so GetFlightInfo and DoGet must agree on the flag.
pub(super) fn tables_schema(include_schema: bool) -> SchemaRef {
    GetTablesBuilder::new(
        None::<String>,
        None::<String>,
        None::<String>,
        Vec::<String>::new(),
        include_schema,
    )
    .schema()
}

pub(super) fn table_types_schema() -> SchemaRef {
    CommandGetTableTypes {}.into_builder().schema()
}

/// Flight SQL reads a missing catalog filter as "every catalog". Lakelet
/// refuses instead: one click on a client's catalog tree would turn into a
/// request against every configured metastore.
pub(super) fn require_catalog(
    catalog_provider_list: &LakeletCatalogProviderList,
    catalog: Option<&str>,
    command: &str,
) -> Result<(String, Arc<dyn LakeletCatalogProvider>), Status> {
    let catalog_name = catalog.filter(|name| !name.is_empty()).ok_or_else(|| {
        Status::invalid_argument(format!("{command} requires a catalog to search"))
    })?;
    let catalog = catalog_provider_list
        .get_catalog(catalog_name)
        .ok_or_else(|| Status::invalid_argument(format!("unknown catalog {catalog_name}")))?;
    Ok((catalog_name.to_string(), catalog))
}

pub(super) fn validate_tables_request(
    catalog_provider_list: &LakeletCatalogProviderList,
    query: &CommandGetTables,
) -> Result<TablesRequest, Status> {
    let table_name_filter_pattern = query
        .table_name_filter_pattern
        .as_deref()
        .filter(|pattern| !pattern.is_empty())
        .map(str::to_string);
    if query.include_schema && table_name_filter_pattern.is_none() {
        // Each schema costs a metastore round trip, so the request has to
        // name the tables it wants rather than ask for every table's schema.
        return Err(Status::invalid_argument(
            "GetTables with include_schema requires a table_name_filter_pattern",
        ));
    }
    let (catalog_name, catalog) =
        require_catalog(catalog_provider_list, query.catalog.as_deref(), "GetTables")?;
    let db_schema_filter_pattern = query
        .db_schema_filter_pattern
        .as_deref()
        .filter(|pattern| !pattern.is_empty())
        .ok_or_else(|| Status::invalid_argument("GetTables requires a db_schema_filter_pattern"))?
        .to_string();
    Ok(TablesRequest {
        catalog_name,
        catalog,
        db_schema_filter_pattern,
        table_name_filter_pattern,
        include_schema: query.include_schema,
    })
}

pub(super) fn catalogs_batch(
    catalog_provider_list: &LakeletCatalogProviderList,
) -> Result<RecordBatch, Status> {
    let mut builder = GetCatalogsBuilder::new();
    for catalog_name in catalog_provider_list.catalog_names() {
        builder.append(catalog_name);
    }
    // The builder sorts, so the unordered catalog map is fine as input.
    builder.build().map_err(Status::from)
}

pub(super) async fn db_schemas_batch(
    catalog_provider_list: &LakeletCatalogProviderList,
    query: &CommandGetDbSchemas,
) -> Result<RecordBatch, Status> {
    let (catalog_name, catalog) = require_catalog(
        catalog_provider_list,
        query.catalog.as_deref(),
        "GetDbSchemas",
    )?;
    let schema_names = catalog
        .list_schema_names()
        .await
        .map_err(df_error_to_status)?;

    let mut builder = GetDbSchemasBuilder::new(
        query.catalog.clone(),
        query.db_schema_filter_pattern.clone(),
    );
    for schema_name in schema_names {
        builder.append(&catalog_name, schema_name);
    }
    // `build` applies the LIKE pattern and sorts the rows.
    builder.build().map_err(Status::from)
}

pub(super) async fn tables_batch(
    catalog_provider_list: &LakeletCatalogProviderList,
    query: &CommandGetTables,
) -> Result<RecordBatch, Status> {
    let request = validate_tables_request(catalog_provider_list, query)?;

    // `db_schema_filter_pattern` is a LIKE pattern rather than a name, and `_`
    // - a wildcard - appears in most real schema names. So narrow the schema
    // list with the same kernel the builder uses, and only pay a
    // `list_table_names` round trip for the schemas that can survive it.
    let schema_names = request
        .catalog
        .list_schema_names()
        .await
        .map_err(df_error_to_status)?;
    let schema_names = filter_like(schema_names, &request.db_schema_filter_pattern)?;

    let mut builder = GetTablesBuilder::new(
        query.catalog.clone(),
        query.db_schema_filter_pattern.clone(),
        query.table_name_filter_pattern.clone(),
        query.table_types.clone(),
        request.include_schema,
    );
    // Dropped by the builder when `include_schema` is false.
    let empty_schema = Schema::empty();
    for schema_name in schema_names {
        let mut table_names = request
            .catalog
            .list_table_names(&schema_name)
            .await
            .map_err(df_error_to_status)?;
        // The same narrowing as for schemas: with `include_schema` every
        // table left here costs a metadata load, so drop the rest first.
        if let Some(pattern) = &request.table_name_filter_pattern {
            table_names = filter_like(table_names, pattern)?;
        }
        let schema_provider = if request.include_schema {
            match request
                .catalog
                .schema(&schema_name)
                .await
                .map_err(df_error_to_status)?
            {
                Some(provider) => Some(provider),
                // Listed a moment ago, gone now: none of its tables can carry
                // a schema, so leave them all out rather than emit empty ones.
                None => continue,
            }
        } else {
            None
        };
        for table_name in table_names {
            let loaded = match &schema_provider {
                Some(provider) => match provider
                    .table(&table_name)
                    .await
                    .map_err(df_error_to_status)?
                {
                    Some(table) => Some(table.schema()),
                    // Listed a moment ago, gone now: leave the row out rather
                    // than fail the whole listing.
                    None => continue,
                },
                None => None,
            };
            builder
                .append(
                    &request.catalog_name,
                    &schema_name,
                    table_name,
                    TABLE_TYPE,
                    loaded.as_deref().unwrap_or(&empty_schema),
                )
                .map_err(Status::from)?;
        }
    }
    builder.build().map_err(Status::from)
}

pub(super) fn table_types_batch() -> Result<RecordBatch, Status> {
    let mut builder = CommandGetTableTypes {}.into_builder();
    builder.append(TABLE_TYPE);
    builder.build().map_err(Status::from)
}

/// Keeps the values matching a SQL LIKE `pattern`, using the same kernel the
/// Flight SQL builders apply when they filter the finished batch.
fn filter_like(values: Vec<String>, pattern: &str) -> Result<Vec<String>, Status> {
    let values = StringArray::from(values);
    let pattern = StringArray::new_scalar(pattern);
    let matches = like(&values, &pattern)
        .map_err(|error| Status::internal(format!("Failed to apply filter pattern: {error}")))?;
    Ok(values
        .iter()
        .zip(matches.iter())
        .filter_map(|(value, matched)| match (value, matched) {
            (Some(value), Some(true)) => Some(value.to_string()),
            _ => None,
        })
        .collect())
}

/// Wraps one already-built metadata batch as a DoGet stream.
pub(super) fn single_batch_response(
    schema: SchemaRef,
    batch: RecordBatch,
) -> Response<<LakeletFlightSqlService as FlightService>::DoGetStream> {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::once(async move { Ok(batch) }))
        .map_err(Status::from);
    Response::new(Box::pin(stream))
}
