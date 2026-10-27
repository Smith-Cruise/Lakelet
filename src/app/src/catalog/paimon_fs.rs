mod ddl;

use crate::catalog::{
    CallProcedureRequest, CatalogConfig, CreateTableOutcome, CreateTableRequest,
    LakeletCatalogProvider,
};
use crate::context::LakeletContext;
use crate::table_format::TableFormat;
use crate::table_format::table_provider_factory::{TableProviderBuilder, parse_table_reference};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::{ResolvedTableReference, Result, SchemaReference, not_impl_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectType, SchemaName, Statement, Value,
};
use datafusion::prelude::{DataFrame, SessionContext};
use lakelet_storage::storage::Storage;
use paimon::catalog::{Catalog, Identifier};
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaimonFSCatalogConfig {
    pub name: String,
    pub warehouse: String,
    #[serde(flatten, default, deserialize_with = "Storage::deserialize_flattened")]
    pub storage: Storage,
}

fn to_datafusion_error(error: paimon::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Paimon filesystem catalog layout: warehouse/<db>.db/<table>.
fn table_location(warehouse: &str, schema_name: &str, table_name: &str) -> String {
    format!(
        "{}/{}.db/{}",
        warehouse.trim_end_matches('/'),
        schema_name,
        table_name
    )
}

pub struct PaimonFSCatalog {
    lakelet_context: Arc<LakeletContext>,
    config: Arc<PaimonFSCatalogConfig>,
    inner_catalog: Arc<FileSystemCatalog>,
}

impl PaimonFSCatalog {
    pub fn try_new(
        lakelet_context: Arc<LakeletContext>,
        config: Arc<PaimonFSCatalogConfig>,
    ) -> Result<Self> {
        let mut options = Options::from_map(config.storage.build_paimon_file_io_properties());
        options.set(
            CatalogOptions::WAREHOUSE,
            config.warehouse.trim_end_matches('/'),
        );
        let inner_catalog = Arc::new(FileSystemCatalog::new(options).map_err(to_datafusion_error)?);
        Ok(Self {
            lakelet_context,
            config,
            inner_catalog,
        })
    }
}

#[async_trait]
impl LakeletCatalogProvider for PaimonFSCatalog {
    async fn call_procedure(
        &self,
        ctx: &SessionContext,
        request: &CallProcedureRequest<'_>,
    ) -> Result<DataFrame> {
        if request.table_format != TableFormat::Paimon {
            return not_impl_err!(
                "Paimon filesystem catalog does not support {:?} procedures",
                request.table_format
            );
        }
        let mut arguments = request.procedure_args.clone();
        let table = &request.table_name;
        // Paimon procedures split the table argument on dots.
        if table.schema.contains('.') || table.table.contains('.') {
            return datafusion::common::plan_err!(
                "Paimon procedure table identifiers cannot contain dots in schema or table names"
            );
        }
        if let FunctionArguments::List(args) = &mut arguments {
            for arg in &mut args.args {
                if let FunctionArg::Named { name, arg, .. } = arg
                    && name.value.eq_ignore_ascii_case("table")
                {
                    *arg = FunctionArgExpr::Expr(Expr::Value(
                        Value::SingleQuotedString(format!("{}.{}", table.schema, table.table))
                            .into(),
                    ));
                    break;
                }
            }
        }
        let catalog: Arc<dyn Catalog> = self.inner_catalog.clone();
        paimon_datafusion::execute_procedure(
            ctx,
            &catalog,
            &self.config.name,
            &request.procedure_name,
            &arguments,
        )
        .await
    }

    fn default_table_format(&self) -> Option<TableFormat> {
        Some(TableFormat::Paimon)
    }

    async fn create_table(&self, request: &CreateTableRequest<'_>) -> Result<CreateTableOutcome> {
        if request.table_format != TableFormat::Paimon {
            return datafusion::common::plan_err!(
                "Paimon filesystem catalog only supports USING PAIMON, got {:?}",
                request.table_format
            );
        }
        let schema = ddl::build_schema(
            request.definition,
            &request.schema,
            request.enable_ident_normalization,
        )?;
        match self
            .inner_catalog
            .create_table(
                &Identifier::new(request.name.schema.as_ref(), request.name.table.as_ref()),
                schema,
                false,
            )
            .await
        {
            Ok(()) => Ok(CreateTableOutcome::Created),
            Err(paimon::Error::TableAlreadyExist { .. }) if request.definition.if_not_exists => {
                Ok(CreateTableOutcome::Skipped)
            }
            Err(error) => Err(to_datafusion_error(error)),
        }
    }

    async fn list_schema_names(&self) -> Result<Vec<String>> {
        self.inner_catalog
            .list_databases()
            .await
            .map_err(to_datafusion_error)
    }

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        self.inner_catalog
            .list_tables(schema_name)
            .await
            .map_err(to_datafusion_error)
    }

    async fn schema_exist(&self, schema_name: &str) -> Result<bool> {
        match self.inner_catalog.get_database(schema_name).await {
            Ok(_) => Ok(true),
            Err(paimon::Error::DatabaseNotExist { .. }) => Ok(false),
            Err(error) => Err(to_datafusion_error(error)),
        }
    }

    async fn create_schema(&self, name: &SchemaReference, stmt: &Statement) -> Result<()> {
        let Statement::CreateSchema {
            schema_name: SchemaName::Simple(_),
            if_not_exists,
            with: None,
            options: None,
            default_collate_spec: None,
            clone: None,
        } = stmt
        else {
            return not_impl_err!(
                "Paimon filesystem catalog only supports CREATE SCHEMA [IF NOT EXISTS] name"
            );
        };
        self.inner_catalog
            .create_database(name.schema_name(), *if_not_exists, HashMap::new())
            .await
            .map_err(to_datafusion_error)
    }

    async fn drop_schema(&self, name: &SchemaReference, stmt: &Statement) -> Result<()> {
        let Statement::Drop {
            object_type: ObjectType::Schema,
            if_exists,
            cascade,
            purge: false,
            temporary: false,
            table: None,
            ..
        } = stmt
        else {
            return not_impl_err!(
                "Paimon filesystem catalog only supports DROP SCHEMA [IF EXISTS] name [CASCADE | RESTRICT]"
            );
        };
        self.inner_catalog
            .drop_database(name.schema_name(), *if_exists, *cascade)
            .await
            .map_err(to_datafusion_error)
    }

    async fn drop_table(&self, name: &ResolvedTableReference, stmt: &Statement) -> Result<()> {
        let Statement::Drop {
            object_type: ObjectType::Table,
            if_exists,
            cascade: false,
            restrict: false,
            purge: false,
            temporary: false,
            table: None,
            ..
        } = stmt
        else {
            return not_impl_err!(
                "Paimon filesystem catalog only supports DROP TABLE [IF EXISTS] name"
            );
        };
        self.inner_catalog
            .drop_table(
                &Identifier::new(name.schema.as_ref(), name.table.as_ref()),
                *if_exists,
            )
            .await
            .map_err(to_datafusion_error)
    }
}

#[async_trait]
impl AsyncCatalogProvider for PaimonFSCatalog {
    async fn schema(&self, schema_name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        Ok(Some(Arc::new(PaimonFSSchema::new(
            self.lakelet_context.clone(),
            self.config.clone(),
            schema_name,
        ))))
    }
}

struct PaimonFSSchema {
    lakelet_context: Arc<LakeletContext>,
    config: Arc<PaimonFSCatalogConfig>,
    schema_name: String,
}

impl PaimonFSSchema {
    pub fn new(
        lakelet_context: Arc<LakeletContext>,
        config: Arc<PaimonFSCatalogConfig>,
        schema_name: &str,
    ) -> Self {
        Self {
            lakelet_context,
            config,
            schema_name: schema_name.to_string(),
        }
    }
}

#[async_trait]
impl AsyncSchemaProvider for PaimonFSSchema {
    async fn table(&self, tbl_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let (table_name, metadata_table_type) = parse_table_reference(tbl_name)?;

        let table_location = table_location(&self.config.warehouse, &self.schema_name, &table_name);
        let table_reference = ResolvedTableReference {
            catalog: self.config.name.as_str().into(),
            schema: self.schema_name.as_str().into(),
            table: table_name.as_str().into(),
        };

        // The table format is fixed by the catalog type, no deduction needed.
        let table_provider_builder = TableProviderBuilder::new(
            self.lakelet_context.clone(),
            table_location,
            table_reference,
            HashMap::new(),
            TableFormat::Paimon,
            CatalogConfig::PaimonFS(self.config.deref().clone()),
        )
        .with_metadata_table_type(metadata_table_type);
        Ok(Some(table_provider_builder.build().await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::LakeletCatalogProviderList;
    use crate::sql::session::ExtendedSessionContext;
    use datafusion::execution::runtime_env::RuntimeEnv;

    async fn query_rows(session: &ExtendedSessionContext, sql: &str) -> Vec<Vec<Option<String>>> {
        use datafusion::arrow::util::display::array_value_to_string;

        let batches = session.sql(sql).await.unwrap().collect().await.unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            (!column.is_null(row))
                                .then(|| array_value_to_string(column.as_ref(), row).unwrap())
                        })
                        .collect()
                })
            })
            .collect()
    }

    #[test]
    fn test_parse_paimon_fs_config_without_storage_block() {
        let config: PaimonFSCatalogConfig = toml::from_str(
            r#"
            name = "paimon_fs_1"
            warehouse = "/tmp/warehouse"
        "#,
        )
        .unwrap();
        assert_eq!(config.name, "paimon_fs_1");
        assert_eq!(config.warehouse, "/tmp/warehouse");
        // No storage block: the object-store schemes fall back to OpenDAL's
        // credential chain (covered in lakelet_storage::storage).
        assert!(config.storage.s3_storage.is_none());
    }

    #[test]
    fn test_parse_paimon_fs_config_with_storage_block() {
        let config: PaimonFSCatalogConfig = toml::from_str(
            r#"
            name = "paimon_fs_1"
            warehouse = "s3://bucket/warehouse"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "ak", secret-key = "sk" }
        "#,
        )
        .unwrap();
        assert_eq!(config.warehouse, "s3://bucket/warehouse");
        assert!(config.storage.build_operator("s3", "bucket").is_ok());
    }

    #[test]
    fn test_parse_paimon_fs_config_requires_warehouse() {
        let result = toml::from_str::<PaimonFSCatalogConfig>(
            r#"
            name = "paimon_fs_1"
        "#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_table_location() {
        assert_eq!(
            table_location("s3://bucket/warehouse", "db1", "t1"),
            "s3://bucket/warehouse/db1.db/t1"
        );
        assert_eq!(
            table_location("s3://bucket/warehouse/", "db1", "t1"),
            "s3://bucket/warehouse/db1.db/t1"
        );
    }

    #[tokio::test]
    async fn test_paimon_fs_catalog_ddl_and_listing() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = PaimonFSCatalogConfig {
            name: "paimon_fs_1".to_string(),
            warehouse: temp_dir.path().to_str().unwrap().to_string(),
            storage: Storage::default(),
        };

        let lakelet_context = Arc::new(LakeletContext::default());
        let catalog =
            Arc::new(PaimonFSCatalog::try_new(lakelet_context.clone(), Arc::new(config)).unwrap());
        let session = ExtendedSessionContext::new(
            Arc::new(LakeletCatalogProviderList::from_catalogs(HashMap::from([
                (
                    "paimon_fs_1".into(),
                    catalog.clone() as Arc<dyn LakeletCatalogProvider>,
                ),
            ]))),
            lakelet_context,
            Arc::new(RuntimeEnv::default()),
        );
        session.sql("USE CATALOG paimon_fs_1").await.unwrap();
        let batches = session
            .sql("CREATE SCHEMA db1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            0
        );
        session
            .sql("CREATE SCHEMA IF NOT EXISTS paimon_fs_1.db1")
            .await
            .unwrap();
        let error = session.sql("CREATE SCHEMA db1").await.unwrap_err();
        assert!(error.to_string().contains("Database db1 already exists"));

        // Prepare table fixtures through Paimon.
        let inner_catalog = catalog.inner_catalog.clone();
        let schema = paimon::spec::Schema::builder()
            .column(
                "id",
                paimon::spec::DataType::Int(paimon::spec::IntType::new()),
            )
            .build()
            .unwrap();
        inner_catalog
            .create_table(&Identifier::new("db1", "t1"), schema.clone(), false)
            .await
            .unwrap();

        assert_eq!(catalog.list_schema_names().await.unwrap(), vec!["db1"]);
        assert_eq!(catalog.list_table_names("db1").await.unwrap(), vec!["t1"]);
        assert!(catalog.schema_exist("db1").await.unwrap());
        assert!(!catalog.schema_exist("db_missing").await.unwrap());

        session.sql("USE paimon_fs_1.db1").await.unwrap();
        // A query replaces the resolved catalog cache; DDL must still use the session.
        session
            .sql("SELECT 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for sql in ["DROP SCHEMA db1", "DROP SCHEMA db1 RESTRICT"] {
            let error = session.sql(sql).await.unwrap_err();
            assert!(error.to_string().contains("Database db1 is not empty"));
            assert!(temp_dir.path().join("db1.db/t1/schema/schema-0").exists());
        }

        for sql in [
            "CREATE SCHEMA unauthorized AUTHORIZATION owner",
            "CREATE SCHEMA AUTHORIZATION owner",
            "CREATE SCHEMA unsupported WITH (key = 'value')",
            "CREATE SCHEMA unsupported OPTIONS (key = 'value')",
            "CREATE SCHEMA unsupported DEFAULT COLLATE 'und:ci'",
            "CREATE SCHEMA unsupported CLONE db1",
            "DROP TABLE t1 CASCADE",
            "DROP TABLE t1 RESTRICT",
            "DROP TABLE t1 PURGE",
            "DROP SCHEMA db1 PURGE",
        ] {
            let error = session.sql(sql).await.unwrap_err();
            assert!(
                matches!(error, DataFusionError::NotImplemented(_)),
                "{sql}: {error}"
            );
            assert_eq!(catalog.list_schema_names().await.unwrap(), vec!["db1"]);
            assert_eq!(catalog.list_table_names("db1").await.unwrap(), vec!["t1"]);
        }

        // Deleting a table removes its data as well as its schema metadata.
        let data_dir = temp_dir.path().join("db1.db/t1/bucket-0");
        std::fs::create_dir(&data_dir).unwrap();
        std::fs::write(data_dir.join("data.parquet"), b"test data").unwrap();
        session.sql("DROP TABLE t1").await.unwrap();
        assert!(!temp_dir.path().join("db1.db/t1").exists());
        assert!(catalog.list_table_names("db1").await.unwrap().is_empty());
        let error = session.sql("DROP TABLE t1").await.unwrap_err();
        assert!(error.to_string().contains("Table db1.t1 does not exist"));
        session.sql("DROP TABLE IF EXISTS db1.t1").await.unwrap();

        inner_catalog
            .create_table(&Identifier::new("db1", "t1"), schema.clone(), false)
            .await
            .unwrap();
        session.sql("DROP TABLE paimon_fs_1.db1.t1").await.unwrap();
        session.sql("DROP SCHEMA db1 RESTRICT").await.unwrap();
        assert!(!temp_dir.path().join("db1.db").exists());
        let error = session.sql("DROP SCHEMA db1").await.unwrap_err();
        assert!(error.to_string().contains("Database db1 does not exist"));
        session.sql("DROP SCHEMA IF EXISTS db1").await.unwrap();

        session.sql("CREATE SCHEMA paimon_fs_1.db1").await.unwrap();
        inner_catalog
            .create_table(&Identifier::new("db1", "t1"), schema, false)
            .await
            .unwrap();
        session
            .sql("DROP SCHEMA IF EXISTS paimon_fs_1.db1 CASCADE")
            .await
            .unwrap();
        assert!(!temp_dir.path().join("db1.db").exists());
        assert!(catalog.list_schema_names().await.unwrap().is_empty());

        session.sql("CREATE SCHEMA `Mixed.Case`").await.unwrap();
        assert!(catalog.schema_exist("Mixed.Case").await.unwrap());
        session.sql("DROP SCHEMA `Mixed.Case`").await.unwrap();
        assert!(catalog.list_schema_names().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_paimon_fs_metadata_tables() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(PaimonFSCatalogConfig {
            name: "paimon_fs_1".to_string(),
            warehouse: temp_dir.path().to_str().unwrap().to_string(),
            storage: Storage::default(),
        });
        let lakelet_context = Arc::new(LakeletContext::default());
        let catalog = PaimonFSCatalog::try_new(lakelet_context.clone(), config.clone()).unwrap();

        let inner_catalog = catalog.inner_catalog.clone();
        inner_catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let schema = paimon::spec::Schema::builder()
            .column(
                "id",
                paimon::spec::DataType::Int(paimon::spec::IntType::new()),
            )
            .build()
            .unwrap();
        inner_catalog
            .create_table(&Identifier::new("db1", "t1"), schema, false)
            .await
            .unwrap();

        let schema_provider = PaimonFSSchema::new(lakelet_context.clone(), config, "db1");
        let ctx = datafusion::prelude::SessionContext::new();

        // `t1$schemas` resolves through paimon-datafusion's system tables and
        // returns the single schema written by create_table.
        let provider = schema_provider.table("t1$schemas").await.unwrap().unwrap();
        let batches = ctx.read_table(provider).unwrap().collect().await.unwrap();
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 1);

        // `t1$options` keeps the key/value shape even with no options set.
        let provider = schema_provider.table("t1$options").await.unwrap().unwrap();
        let field_names: Vec<_> = provider
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(field_names, vec!["key", "value"]);

        // `t1$snapshots` is empty before any write but must still scan.
        let provider = schema_provider
            .table("t1$snapshots")
            .await
            .unwrap()
            .unwrap();
        let batches = ctx.read_table(provider).unwrap().collect().await.unwrap();
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 0);

        // `history` is an Iceberg-only metadata table.
        let error = schema_provider.table("t1$history").await.unwrap_err();
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{error:?}"
        );

        for (suffix, expected_fields) in [
            (
                "aggregation_fields",
                vec![
                    "field_name",
                    "field_type",
                    "function",
                    "function_options",
                    "comment",
                ],
            ),
            ("audit_log", vec!["rowkind", "id"]),
            (
                "buckets",
                vec![
                    "partition",
                    "bucket",
                    "record_count",
                    "file_size_in_bytes",
                    "file_count",
                    "last_update_time",
                ],
            ),
            ("consumers", vec!["consumer_id", "next_snapshot_id"]),
        ] {
            let provider = schema_provider
                .table(&format!("t1${suffix}"))
                .await
                .unwrap()
                .unwrap();
            let schema = provider.schema();
            let fields: Vec<_> = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            assert_eq!(fields, expected_fields, "{suffix}");
            let batches = ctx.read_table(provider).unwrap().collect().await.unwrap();
            let expected_rows = usize::from(suffix == "aggregation_fields");
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                expected_rows,
                "{suffix}"
            );
        }

        let session = ExtendedSessionContext::new(
            Arc::new(LakeletCatalogProviderList::from_catalogs(HashMap::from([
                (
                    "paimon_fs_1".into(),
                    Arc::new(catalog) as Arc<dyn LakeletCatalogProvider>,
                ),
            ]))),
            lakelet_context,
            Arc::new(RuntimeEnv::default()),
        );
        session.sql("USE paimon_fs_1.db1").await.unwrap();

        // Consumer progress is written by external readers, not SQL DDL.
        assert!(
            query_rows(&session, "SELECT * FROM t1$consumers")
                .await
                .is_empty()
        );
        let consumer_dir = temp_dir.path().join("db1.db/t1/consumer");
        std::fs::create_dir_all(&consumer_dir).unwrap();
        std::fs::write(
            consumer_dir.join("consumer-reader1"),
            r#"{"nextSnapshot":5}"#,
        )
        .unwrap();
        std::fs::write(
            consumer_dir.join("consumer-reader2"),
            r#"{"nextSnapshot":8}"#,
        )
        .unwrap();
        assert_eq!(
            query_rows(
                &session,
                "SELECT consumer_id, next_snapshot_id FROM t1$consumers ORDER BY consumer_id"
            )
            .await,
            vec![
                vec![Some("reader1".into()), Some("5".into())],
                vec![Some("reader2".into()), Some("8".into())],
            ]
        );
        assert_eq!(
            query_rows(
                &session,
                "SELECT next_snapshot_id FROM t1$consumers WHERE consumer_id = 'reader2'"
            )
            .await,
            vec![vec![Some("8".into())]]
        );

        session
            .sql(
                "CREATE TABLE agg_fields (
                    id INT,
                    total INT64 COMMENT 'Total amount',
                    PRIMARY KEY (id)
                ) USING PAIMON WITH (
                    'bucket' = '1',
                    'merge-engine' = 'aggregation',
                    'fields.total.aggregate-function' = 'sum'
                )",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            query_rows(
                &session,
                "SELECT field_name, field_type, function, function_options, comment
                 FROM agg_fields$aggregation_fields WHERE field_name = 'total'"
            )
            .await,
            vec![vec![
                Some("total".into()),
                Some("BIGINT".into()),
                Some("[sum]".into()),
                Some("[fields.total.aggregate-function]".into()),
                Some("Total amount".into()),
            ]]
        );
    }

    #[tokio::test]
    async fn test_paimon_fs_insert_and_read_back() {
        use datafusion::arrow::array::UInt64Array;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(PaimonFSCatalogConfig {
            name: "paimon_fs_1".to_string(),
            warehouse: temp_dir.path().to_str().unwrap().to_string(),
            storage: Storage::default(),
        });
        let lakelet_context = Arc::new(LakeletContext::default());
        let catalog = Arc::new(PaimonFSCatalog::try_new(lakelet_context.clone(), config).unwrap());
        let session = ExtendedSessionContext::new(
            Arc::new(LakeletCatalogProviderList::from_catalogs(HashMap::from([
                (
                    "paimon_fs_1".into(),
                    catalog.clone() as Arc<dyn LakeletCatalogProvider>,
                ),
            ]))),
            lakelet_context,
            Arc::new(RuntimeEnv::default()),
        );
        session.sql("USE CATALOG paimon_fs_1").await.unwrap();
        session.sql("CREATE SCHEMA db1").await.unwrap();
        session.sql("USE paimon_fs_1.db1").await.unwrap();
        // Clear resolved query catalogs before DDL to exercise session routing.
        session
            .sql("SELECT 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batches = session
            .sql(
                "CREATE TABLE orders (
                    id BIGINT COMMENT 'Order ID',
                    dt STRING,
                    amount DECIMAL(18, 2),
                    event_time TIMESTAMP(6),
                    tags ARRAY<STRING>,
                    attributes MAP<STRING, STRING>,
                    address STRUCT<city STRING, postcode STRING>,
                    note STRING,
                    PRIMARY KEY (id, dt)
                ) PARTITIONED BY (dt) USING PAIMON WITH ('bucket' = '2')",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            0
        );

        // Reload persisted metadata rather than inspecting the CREATE TABLE AST.
        let table = catalog
            .inner_catalog
            .get_table(&Identifier::new("db1", "orders"))
            .await
            .unwrap();
        let schema = table.schema();
        assert_eq!(schema.primary_keys(), &["id", "dt"]);
        assert_eq!(schema.partition_keys(), &["dt"]);
        assert_eq!(
            schema.options().get("bucket").map(String::as_str),
            Some("2")
        );
        assert_eq!(schema.fields()[0].description(), Some("Order ID"));
        assert!(!schema.fields()[0].data_type().is_nullable());
        assert!(!schema.fields()[1].data_type().is_nullable());

        let definition = query_rows(&session, "SHOW CREATE TABLE orders").await;
        assert_eq!(definition.len(), 1);
        assert!(
            definition[0][0]
                .as_ref()
                .unwrap()
                .ends_with("\nWITH (\n  'bucket' = '2'\n)")
        );

        // INSERT goes through the Lakelet wrapper, which must forward to
        // paimon-datafusion's sink; the result is a single row count.
        let batches = session
            .sql(
                "INSERT INTO orders VALUES
                    (2, '2026-01-03', 56.78, TIMESTAMP '2026-01-03 04:05:06.654321',
                     make_array('sale'), make_map('source', 'app'),
                     named_struct('city', 'Beijing', 'postcode', '100000'), NULL),
                    (1, '2026-01-02', 12.34, TIMESTAMP '2026-01-02 03:04:05.123456',
                     make_array('new', 'paid'),
                     make_map('source', 'web', 'region', 'east'),
                     named_struct('city', 'Shanghai', 'postcode', '200000'), 'first order')",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(count.value(0), 2);

        let procedure_tables = [
            ("orders", "unqualified_tag"),
            ("db1.orders", "schema_qualified_tag"),
            ("paimon_fs_1.db1.orders", "catalog_qualified_tag"),
        ];
        let tag_manager = table.tag_manager();
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        for (table_name, tag_name) in procedure_tables {
            assert_eq!(
                query_rows(
                    &session,
                    &format!(
                        "CALL paimon.create_tag(table => '{table_name}', tag => '{tag_name}')"
                    ),
                )
                .await,
                vec![vec![Some("OK".into())]]
            );
            let tag = tag_manager.get(tag_name).await.unwrap().unwrap();
            assert_eq!(tag.id(), snapshot.id());
        }
        for (table_name, tag_name) in procedure_tables {
            assert_eq!(
                query_rows(
                    &session,
                    &format!(
                        "CALL paimon.delete_tag(table => '{table_name}', tag => '{tag_name}')"
                    ),
                )
                .await,
                vec![vec![Some("OK".into())]]
            );
            assert!(!tag_manager.tag_exists(tag_name).await.unwrap());
        }

        // Lakelet rebuilds the provider per statement, so read back through a
        // fresh one to make sure the commit is visible from the warehouse.
        let rows = query_rows(&session, "SELECT * FROM orders ORDER BY id, dt").await;
        let expected: Vec<Vec<Option<String>>> = [
            [
                Some("1"),
                Some("2026-01-02"),
                Some("12.34"),
                Some("2026-01-02T03:04:05.123456"),
                Some("[new, paid]"),
                Some("{source: web, region: east}"),
                Some("{city: Shanghai, postcode: 200000}"),
                Some("first order"),
            ],
            [
                Some("2"),
                Some("2026-01-03"),
                Some("56.78"),
                Some("2026-01-03T04:05:06.654321"),
                Some("[sale]"),
                Some("{source: app}"),
                Some("{city: Beijing, postcode: 100000}"),
                None,
            ],
        ]
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| value.map(str::to_owned))
                .collect()
        })
        .collect();
        assert_eq!(rows, expected);

        let error = session
            .sql("CREATE TABLE orders (replacement STRING)")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("already exists"));
        session
            .sql("CREATE TABLE IF NOT EXISTS orders (replacement STRING)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let unchanged = catalog
            .inner_catalog
            .get_table(&Identifier::new("db1", "orders"))
            .await
            .unwrap();
        assert_eq!(unchanged.schema().fields(), schema.fields());
        assert_eq!(unchanged.schema().primary_keys(), schema.primary_keys());
        assert_eq!(unchanged.schema().partition_keys(), schema.partition_keys());
        assert_eq!(unchanged.schema().options(), schema.options());
        assert_eq!(
            query_rows(&session, "SELECT * FROM orders ORDER BY id, dt").await,
            expected
        );

        for sql in [
            "CREATE TABLE invalid (id INT DEFAULT 1)",
            "CREATE TABLE invalid (id INT, PRIMARY KEY (missing))",
        ] {
            assert!(session.sql(sql).await.is_err(), "{sql}");
            assert!(!temp_dir.path().join("db1.db/invalid").exists());
            assert_eq!(
                catalog.list_table_names("db1").await.unwrap(),
                vec!["orders"]
            );
        }

        let audit_rows =
            query_rows(&session, "SELECT * FROM orders$audit_log ORDER BY id, dt").await;
        let expected_audit: Vec<_> = expected
            .into_iter()
            .map(|mut row| {
                row.insert(0, Some("+I".into()));
                row
            })
            .collect();
        assert_eq!(audit_rows, expected_audit);
        assert_eq!(
            query_rows(
                &session,
                "SELECT id FROM orders$audit_log WHERE rowkind = '+I' AND id = 2"
            )
            .await,
            vec![vec![Some("2".into())]]
        );

        // Bucket statistics must agree with the committed data files.
        let bucket_rows = query_rows(
            &session,
            "SELECT partition, bucket, record_count, file_size_in_bytes, file_count
             FROM orders$buckets ORDER BY partition, bucket",
        )
        .await;
        let file_rows = query_rows(
            &session,
            "SELECT partition, bucket, SUM(record_count), SUM(file_size_in_bytes), COUNT(*)
             FROM orders$data_files GROUP BY partition, bucket ORDER BY partition, bucket",
        )
        .await;
        assert_eq!(bucket_rows.len(), 2);
        assert_eq!(bucket_rows, file_rows);

        // Both derived forms share the same schema conversion, including nested
        // columns and nullable fields. LIKE must not copy keys or properties.
        session
            .sql("CREATE TABLE columns_only LIKE orders")
            .await
            .unwrap();
        let columns_only = catalog
            .inner_catalog
            .get_table(&Identifier::new("db1", "columns_only"))
            .await
            .unwrap();
        assert!(columns_only.schema().primary_keys().is_empty());
        assert!(columns_only.schema().partition_keys().is_empty());
        assert!(columns_only.schema().options().is_empty());
        let source_schema = session.sql("SELECT * FROM orders").await.unwrap();
        for (index, (original, copied)) in schema
            .fields()
            .iter()
            .zip(columns_only.schema().fields())
            .enumerate()
        {
            assert_eq!(original.name(), copied.name());
            // LIKE follows the provider's read schema, which can be more
            // nullable than the stored schema after schema-evolution casts.
            assert_eq!(
                &original
                    .data_type()
                    .copy_with_nullable(source_schema.schema().field(index).is_nullable(),)
                    .unwrap(),
                copied.data_type()
            );
            assert!(copied.description().is_none());
        }
        assert!(
            query_rows(&session, "SELECT * FROM columns_only")
                .await
                .is_empty()
        );
        session
            .sql("CREATE TABLE IF NOT EXISTS columns_only LIKE orders")
            .await
            .unwrap();

        let result = session
            .sql(
                "CREATE TABLE populated PARTITIONED BY (dt)
             WITH ('primary-key' = 'id,dt', 'bucket' = '2') AS SELECT * FROM orders",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert_eq!(
            query_rows(&session, "SELECT * FROM populated ORDER BY id, dt").await,
            rows
        );
        let populated = catalog
            .inner_catalog
            .get_table(&Identifier::new("db1", "populated"))
            .await
            .unwrap();
        assert_eq!(populated.schema().primary_keys(), &["id", "dt"]);
        assert_eq!(populated.schema().partition_keys(), &["dt"]);
        session.sql(
            "CREATE TABLE IF NOT EXISTS populated AS SELECT CAST(note AS INT) AS id FROM orders"
        ).await.unwrap();
        assert_eq!(
            query_rows(&session, "SELECT * FROM populated ORDER BY id, dt").await,
            rows
        );

        session
            .sql(
                "CREATE TABLE configured_like LIKE orders PARTITIONED BY (dt)
             WITH ('primary-key' = 'id,dt', 'bucket' = '1')",
            )
            .await
            .unwrap();
        let configured = catalog
            .inner_catalog
            .get_table(&Identifier::new("db1", "configured_like"))
            .await
            .unwrap();
        assert_eq!(configured.schema().primary_keys(), &["id", "dt"]);
        assert_eq!(configured.schema().partition_keys(), &["dt"]);
        assert_eq!(configured.schema().options().get("bucket").unwrap(), "1");
        assert!(
            query_rows(&session, "SELECT * FROM configured_like")
                .await
                .is_empty()
        );

        session
            .sql("CREATE TABLE no_rows AS SELECT * FROM orders WHERE false")
            .await
            .unwrap();
        assert!(
            query_rows(&session, "SELECT * FROM no_rows")
                .await
                .is_empty()
        );
        session.sql(
            "CREATE TABLE totals AS SELECT count(*) AS `RowCount`, sum(amount) AS total FROM orders"
        ).await.unwrap();
        assert_eq!(
            query_rows(&session, "SELECT `RowCount`, total FROM totals").await,
            vec![vec![Some("2".into()), Some("69.12".into())]]
        );
        session
            .sql("CREATE TABLE constant AS SELECT 'hello' AS `Kept`")
            .await
            .unwrap();
        assert_eq!(
            query_rows(&session, "SELECT `Kept` FROM constant").await,
            vec![vec![Some("hello".into())]]
        );

        for sql in [
            "CREATE TABLE invalid AS SELECT NULL AS id",
            "CREATE TABLE invalid AS SELECT * FROM orders a CROSS JOIN orders b",
            "CREATE TABLE invalid PARTITIONED BY (missing) AS SELECT id FROM orders",
            "CREATE TABLE invalid USING ICEBERG AS SELECT id FROM orders",
            "CREATE TABLE IF NOT EXISTS populated AS SELECT * FROM missing",
            "CREATE TABLE IF NOT EXISTS columns_only LIKE missing",
        ] {
            assert!(session.sql(sql).await.is_err(), "{sql}");
            assert!(!temp_dir.path().join("db1.db/invalid").exists());
        }
        assert!(
            session
                .sql("CREATE TABLE failed_write AS SELECT CAST(note AS INT) AS id FROM orders")
                .await
                .is_err()
        );
        assert!(
            catalog
                .inner_catalog
                .get_table(&Identifier::new("db1", "failed_write"))
                .await
                .is_ok()
        );

        session.sql("DROP TABLE populated").await.unwrap();
        session.sql("DROP TABLE columns_only").await.unwrap();
        assert_eq!(
            query_rows(&session, "SELECT * FROM orders ORDER BY id, dt").await,
            rows
        );
    }
}
