use super::ExtendedSessionContext;
use crate::catalog::{CreateTableOutcome, CreateTableRequest, CreateTableSchema};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::{
    ResolvedTableReference, Result, SchemaReference, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::logical_expr::sqlparser::ast::{
    CreateTable, CreateTableLikeKind, HiveIOFormat, Ident, Insert, ObjectName, ObjectType,
    SchemaName, Statement, TableObject,
};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::planner::{IdentNormalizer, object_name_to_table_reference};
use std::collections::HashSet;
use std::sync::Arc;

impl ExtendedSessionContext {
    pub(super) fn resolve_ddl_table_name(
        &self,
        name: &ObjectName,
    ) -> Result<ResolvedTableReference> {
        let state = self.session_context.state();
        let options = state.config().options();
        Ok(object_name_to_table_reference(
            name.clone(),
            options.sql_parser.enable_ident_normalization,
        )?
        .resolve(
            &options.catalog.default_catalog,
            &options.catalog.default_schema,
        ))
    }

    pub(super) async fn handle_create_table_stmt(&self, stmt: &CreateTable) -> Result<DataFrame> {
        let name = self.resolve_ddl_table_name(&stmt.name)?;
        let provider = self
            .catalog_provider_list
            .get_catalog(&name.catalog)
            .ok_or_else(|| plan_datafusion_err!("unknown catalog {}", name.catalog))?;

        // Creation targets are not query inputs; resolve only the source below.
        let table_format = match stmt
            .hive_formats
            .as_ref()
            .and_then(|format| format.storage.as_ref())
        {
            Some(HiveIOFormat::Using { format }) => format.value.parse()?,
            Some(_) => return not_impl_err!("Use USING to select the table format"),
            None => provider.default_table_format().ok_or_else(|| {
                plan_datafusion_err!("CREATE TABLE {name} requires USING <table_format>")
            })?,
        };
        let enable_ident_normalization = self
            .session_context
            .state()
            .config()
            .options()
            .sql_parser
            .enable_ident_normalization;
        let mut definition = stmt.clone();
        let query = definition.query.take();
        let like = definition.like.take();
        if query.is_some() && like.is_some() {
            return plan_err!("CREATE TABLE cannot combine AS query and LIKE");
        }
        if (query.is_some() || like.is_some()) && !stmt.columns.is_empty() {
            return not_impl_err!("CTAS and LIKE do not support explicit column definitions");
        }
        if (query.is_some() || like.is_some())
            && (stmt.or_replace
                || stmt.temporary
                || stmt.location.is_some()
                || stmt
                    .hive_formats
                    .as_ref()
                    .is_some_and(|format| format.location.is_some()))
        {
            return not_impl_err!("CTAS and LIKE do not support REPLACE, TEMPORARY or LOCATION");
        }

        let schema = if let Some(query) = &query {
            let plan = self
                .plan_statement(Box::new(Statement::Query(query.clone())))
                .await?;
            if matches!(plan, LogicalPlan::Ddl(_)) {
                return not_impl_err!("CTAS does not support SELECT INTO");
            }
            // Analyze types and validate the query before creating a persistent table.
            let plan = self.session_context.state().optimize(&plan)?;
            let schema = derived_table_schema(plan.schema().as_arrow())?;
            CreateTableSchema::Derived(schema)
        } else if let Some(like) = like {
            let CreateTableLikeKind::Plain(like) = like else {
                return not_impl_err!("Use CREATE TABLE target LIKE source");
            };
            if like.defaults.is_some() {
                return not_impl_err!(
                    "LIKE only copies columns; INCLUDING/EXCLUDING is not supported"
                );
            }
            let source = self.resolve_ddl_table_name(&like.name)?;
            let source = self.get_table_provider(&source).await?;
            CreateTableSchema::Derived(derived_table_schema(source.schema().as_ref())?)
        } else {
            CreateTableSchema::Declared
        };
        let outcome = provider
            .create_table(&CreateTableRequest {
                name: name.clone(),
                table_format,
                definition: &definition,
                schema,
                enable_ident_normalization,
            })
            .await?;
        if outcome == CreateTableOutcome::Created
            && let Some(query) = query
        {
            // Replan the original query as a regular INSERT so DataFusion owns
            // column validation, casts and aliases. Only this plan executes data.
            let insert = Statement::Insert(Insert {
                insert_token: AttachedToken::empty(),
                optimizer_hints: vec![],
                or: None,
                ignore: false,
                into: true,
                table: TableObject::TableName(ObjectName::from(vec![
                    Ident::with_quote('`', name.catalog.as_ref()),
                    Ident::with_quote('`', name.schema.as_ref()),
                    Ident::with_quote('`', name.table.as_ref()),
                ])),
                table_alias: None,
                columns: vec![],
                overwrite: false,
                source: Some(query),
                assignments: vec![],
                partitioned: None,
                after_columns: vec![],
                has_table_keyword: false,
                on: None,
                returning: None,
                output: None,
                replace_into: false,
                priority: None,
                insert_alias: None,
                settings: None,
                format_clause: None,
                multi_table_insert_type: None,
                multi_table_into_clauses: vec![],
                multi_table_when_clauses: vec![],
                multi_table_else_clause: None,
            });
            let insert = self.plan_statement(Box::new(insert)).await?;
            // Collect only the sink's row count; query batches stream into the sink.
            // A failure leaves the newly created table in place.
            self.session_context
                .execute_logical_plan(insert)
                .await?
                .collect()
                .await?;
        }
        self.session_context
            .execute_logical_plan(LogicalPlanBuilder::empty(false).build()?)
            .await
    }

    async fn get_table_provider(
        &self,
        name: &ResolvedTableReference,
    ) -> Result<Arc<dyn TableProvider>> {
        let catalog = self
            .catalog_provider_list
            .get_catalog(&name.catalog)
            .ok_or_else(|| plan_datafusion_err!("unknown catalog {}", name.catalog))?;
        let schema = catalog.schema(&name.schema).await?.ok_or_else(|| {
            plan_datafusion_err!("unknown schema {}.{}", name.catalog, name.schema)
        })?;
        schema
            .table(&name.table)
            .await?
            .ok_or_else(|| plan_datafusion_err!("Table {name} does not exist"))
    }

    pub(super) async fn handle_create_schema_stmt(
        &self,
        schema_name: &SchemaName,
        stmt: &Statement,
    ) -> Result<DataFrame> {
        let name = match schema_name {
            SchemaName::Simple(name) | SchemaName::NamedAuthorization(name, _) => name.clone(),
            SchemaName::UnnamedAuthorization(ident) => ObjectName::from(vec![ident.clone()]),
        };
        let (catalog, name) = self.resolve_ddl_schema_name(&name)?;
        let provider = self
            .catalog_provider_list
            .get_catalog(&catalog)
            .ok_or_else(|| plan_datafusion_err!("unknown catalog {catalog}"))?;
        provider.create_schema(&name, stmt).await?;
        self.session_context
            .execute_logical_plan(LogicalPlanBuilder::empty(false).build()?)
            .await
    }

    fn resolve_ddl_schema_name(&self, name: &ObjectName) -> Result<(String, SchemaReference)> {
        let state = self.session_context.state();
        let options = state.config().options();
        let normalizer = IdentNormalizer::new(options.sql_parser.enable_ident_normalization);
        let parts = name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .cloned()
                    .map(|ident| normalizer.normalize(ident))
                    .ok_or_else(|| plan_datafusion_err!("Schema DDL requires identifier names"))
            })
            .collect::<Result<Vec<_>>>()?;
        let (catalog, schema) = match parts.as_slice() {
            [schema] => (&options.catalog.default_catalog, schema),
            [catalog, schema] => (catalog, schema),
            _ => return plan_err!("unsupported schema name: {name}"),
        };
        let name = SchemaReference::Full {
            catalog: catalog.as_str().into(),
            schema: schema.as_str().into(),
        };
        Ok((catalog.clone(), name))
    }

    pub(super) async fn handle_drop_stmt(
        &self,
        object_type: &ObjectType,
        names: &[ObjectName],
        stmt: &Statement,
    ) -> Result<DataFrame> {
        let [name] = names else {
            return not_impl_err!("DROP {object_type} requires exactly one object name");
        };
        match object_type {
            ObjectType::Table => {
                let name = self.resolve_ddl_table_name(name)?;
                let provider = self
                    .catalog_provider_list
                    .get_catalog(&name.catalog)
                    .ok_or_else(|| plan_datafusion_err!("unknown catalog {}", name.catalog))?;
                provider.drop_table(&name, stmt).await?;
            }
            ObjectType::Schema => {
                let (catalog, name) = self.resolve_ddl_schema_name(name)?;
                let provider = self
                    .catalog_provider_list
                    .get_catalog(&catalog)
                    .ok_or_else(|| plan_datafusion_err!("unknown catalog {catalog}"))?;
                provider.drop_schema(&name, stmt).await?;
            }
            _ => return not_impl_err!("DROP {object_type} is not supported"),
        }
        self.session_context
            .execute_logical_plan(LogicalPlanBuilder::empty(false).build()?)
            .await
    }
}

/// Copy the portable column schema, excluding source IDs, comments and other metadata.
fn derived_table_schema(schema: &Schema) -> Result<SchemaRef> {
    let mut names = HashSet::new();
    for field in schema.fields() {
        if !names.insert(field.name()) {
            return plan_err!("Duplicate column: {}", field.name());
        }
    }
    Ok(Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| column_without_metadata(field))
            .collect::<Vec<_>>(),
    )))
}

fn column_without_metadata(field: &Field) -> Field {
    Field::new(
        field.name(),
        type_without_metadata(field.data_type()),
        field.is_nullable(),
    )
}

fn type_without_metadata(data_type: &DataType) -> DataType {
    let field = |field: &Field| Arc::new(column_without_metadata(field));
    match data_type {
        DataType::List(inner) => DataType::List(field(inner)),
        DataType::LargeList(inner) => DataType::LargeList(field(inner)),
        DataType::ListView(inner) => DataType::ListView(field(inner)),
        DataType::LargeListView(inner) => DataType::LargeListView(field(inner)),
        DataType::FixedSizeList(inner, size) => DataType::FixedSizeList(field(inner), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(|f| field(f)).collect()),
        DataType::Map(inner, sorted) => DataType::Map(field(inner), *sorted),
        DataType::Union(fields, mode) => {
            DataType::Union(fields.iter().map(|(id, f)| (id, field(f))).collect(), *mode)
        }
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(type_without_metadata(key)),
            Box::new(type_without_metadata(value)),
        ),
        DataType::RunEndEncoded(ends, values) => {
            DataType::RunEndEncoded(field(ends), field(values))
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{LakeletCatalogProvider, LakeletCatalogProviderList};
    use crate::context::LakeletContext;
    use crate::parser::ExtendedParser;
    use crate::statements::ExtendedStatement;
    use async_trait::async_trait;
    use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider};
    use datafusion::common::{DataFusionError, ResolvedTableReference, not_impl_err};
    use datafusion::execution::runtime_env::RuntimeEnv;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct DerivedCatalog {
        tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
    }

    #[async_trait]
    impl AsyncSchemaProvider for DerivedCatalog {
        async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            Ok(self.tables.lock().unwrap().get(name).cloned())
        }
    }

    #[async_trait]
    impl AsyncCatalogProvider for DerivedCatalog {
        async fn schema(&self, _: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
            // Snapshot the catalog; new tables must be loaded after creation.
            Ok(Some(Arc::new(Self {
                tables: Mutex::new(self.tables.lock().unwrap().clone()),
            })))
        }
    }

    #[async_trait]
    impl LakeletCatalogProvider for DerivedCatalog {
        fn default_table_format(&self) -> Option<crate::table_format::TableFormat> {
            Some(crate::table_format::TableFormat::Hive)
        }

        async fn list_schema_names(&self) -> Result<Vec<String>> {
            Ok(vec!["db".into()])
        }

        async fn list_table_names(&self, _: &str) -> Result<Vec<String>> {
            Ok(self.tables.lock().unwrap().keys().cloned().collect())
        }

        async fn schema_exist(&self, _: &str) -> Result<bool> {
            Ok(true)
        }

        async fn create_table(
            &self,
            request: &CreateTableRequest<'_>,
        ) -> Result<CreateTableOutcome> {
            use datafusion::datasource::MemTable;
            assert!(request.definition.query.is_none());
            assert!(request.definition.like.is_none());
            let CreateTableSchema::Derived(schema) = &request.schema else {
                panic!("expected a derived schema");
            };
            let mut tables = self.tables.lock().unwrap();
            if tables.contains_key(request.name.table.as_ref()) {
                return if request.definition.if_not_exists {
                    Ok(CreateTableOutcome::Skipped)
                } else {
                    plan_err!("Table {} already exists", request.name)
                };
            }
            tables.insert(
                request.name.table.to_string(),
                Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?),
            );
            Ok(CreateTableOutcome::Created)
        }
    }

    #[tokio::test]
    async fn test_derived_creation_is_catalog_independent() -> Result<()> {
        let source = Arc::new(DerivedCatalog::default());
        let destination = Arc::new(DerivedCatalog::default());
        let session = ExtendedSessionContext::new(
            Arc::new(LakeletCatalogProviderList::from_catalogs(HashMap::from([
                (
                    "source".into(),
                    source.clone() as Arc<dyn LakeletCatalogProvider>,
                ),
                (
                    "destination".into(),
                    destination.clone() as Arc<dyn LakeletCatalogProvider>,
                ),
            ]))),
            Arc::new(LakeletContext::default()),
            Arc::new(RuntimeEnv::default()),
        );
        session.sql("USE source.db").await?;
        session
            .sql("CREATE TABLE seed AS SELECT 1 AS id, 'bad' AS text")
            .await?;
        session
            .sql(
                "CREATE TABLE destination.db.`Table.Name` AS
                  WITH selected AS (SELECT id AS `Kept` FROM seed WHERE id = 1)
                  SELECT * FROM selected",
            )
            .await?;
        let copied = session
            .sql("SELECT * FROM destination.db.`Table.Name`")
            .await?
            .collect()
            .await?;
        assert_eq!(copied[0].schema().field(0).name(), "Kept");
        assert_eq!(copied.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        session
            .sql("CREATE TABLE destination.db.empty LIKE seed")
            .await?;
        let source_schema = source.table("seed").await?.unwrap().schema();
        let empty_schema = destination.table("empty").await?.unwrap().schema();
        assert_eq!(source_schema, empty_schema);
        let empty = session
            .sql("SELECT * FROM destination.db.empty")
            .await?
            .collect()
            .await?;
        assert_eq!(empty.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

        // The cast fails only at execution; a skipped create must never run it.
        session
            .sql("CREATE TABLE IF NOT EXISTS seed AS SELECT CAST(text AS INT) AS id FROM seed")
            .await?;
        session
            .sql("CREATE TABLE IF NOT EXISTS seed LIKE destination.db.empty")
            .await?;
        let seed = session.sql("SELECT * FROM seed").await?.collect().await?;
        assert_eq!(seed.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        assert!(
            session
                .sql("CREATE TABLE failed AS SELECT CAST(text AS INT) AS id FROM seed")
                .await
                .is_err()
        );
        assert!(source.table("failed").await?.is_some());
        for sql in [
            "CREATE TABLE invalid AS SELECT * FROM seed a CROSS JOIN seed b",
            "CREATE TABLE invalid AS SELECT * FROM missing",
            "CREATE TABLE invalid LIKE missing",
            "CREATE TABLE IF NOT EXISTS seed AS SELECT * FROM missing",
            "CREATE TABLE IF NOT EXISTS seed LIKE missing",
            "CREATE TABLE invalid (id INT) AS SELECT 1",
            "CREATE TABLE invalid (id INT) LIKE seed",
            "CREATE OR REPLACE TABLE invalid AS SELECT 1 AS id",
            "CREATE TEMPORARY TABLE invalid LIKE seed",
            "CREATE TABLE invalid LIKE seed INCLUDING DEFAULTS",
            "CREATE TABLE invalid AS SELECT * INTO another FROM seed",
        ] {
            assert!(session.sql(sql).await.is_err(), "{sql}");
            assert!(source.table("invalid").await?.is_none(), "{sql}");
        }
        Ok(())
    }

    #[test]
    fn test_derived_schema_copies_only_columns() -> Result<()> {
        let metadata = HashMap::from([("PARQUET:field_id".into(), "42".into())]);
        let nested = Field::new("Nested", DataType::Int64, false).with_metadata(metadata.clone());
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("Kept", DataType::Struct(vec![nested].into()), true)
                    .with_metadata(metadata.clone()),
            ],
            metadata,
        );
        let copied = derived_table_schema(&schema)?;
        assert_eq!(
            copied.as_ref(),
            &Schema::new(vec![Field::new(
                "Kept",
                DataType::Struct(vec![Field::new("Nested", DataType::Int64, false)].into()),
                true,
            )])
        );
        Ok(())
    }

    #[derive(Debug, PartialEq)]
    enum DdlCall {
        Table(ResolvedTableReference, Box<CreateTable>),
        Schema(SchemaReference, Box<Statement>),
        DropTable(ResolvedTableReference, Box<Statement>),
        DropSchema(SchemaReference, Box<Statement>),
    }

    #[derive(Default)]
    struct TestingCatalog {
        calls: Mutex<Vec<DdlCall>>,
    }

    #[async_trait]
    impl AsyncCatalogProvider for TestingCatalog {
        async fn schema(&self, _: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
            panic!("DDL routing must not resolve query inputs");
        }
    }

    #[async_trait]
    impl LakeletCatalogProvider for TestingCatalog {
        fn default_table_format(&self) -> Option<crate::table_format::TableFormat> {
            Some(crate::table_format::TableFormat::Paimon)
        }

        async fn list_schema_names(&self) -> Result<Vec<String>> {
            Ok(vec!["sales".into()])
        }

        async fn list_table_names(&self, _: &str) -> Result<Vec<String>> {
            panic!("DDL routing must not list tables");
        }

        async fn schema_exist(&self, name: &str) -> Result<bool> {
            // Only USE needs this lookup.
            assert_eq!(name, "sales");
            Ok(true)
        }

        async fn create_table(
            &self,
            request: &CreateTableRequest<'_>,
        ) -> Result<CreateTableOutcome> {
            self.calls.lock().unwrap().push(DdlCall::Table(
                request.name.clone(),
                Box::new(request.definition.clone()),
            ));
            not_impl_err!("recorded CREATE TABLE")
        }

        async fn create_schema(&self, name: &SchemaReference, stmt: &Statement) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(DdlCall::Schema(name.clone(), Box::new(stmt.clone())));
            not_impl_err!("recorded CREATE SCHEMA")
        }

        async fn drop_table(&self, name: &ResolvedTableReference, stmt: &Statement) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(DdlCall::DropTable(name.clone(), Box::new(stmt.clone())));
            not_impl_err!("recorded DROP TABLE")
        }

        async fn drop_schema(&self, name: &SchemaReference, stmt: &Statement) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(DdlCall::DropSchema(name.clone(), Box::new(stmt.clone())));
            not_impl_err!("recorded DROP SCHEMA")
        }
    }

    fn recording_session() -> (
        ExtendedSessionContext,
        Arc<TestingCatalog>,
        Arc<TestingCatalog>,
    ) {
        let local = Arc::new(TestingCatalog::default());
        let other = Arc::new(TestingCatalog::default());
        let catalogs: HashMap<String, Arc<dyn LakeletCatalogProvider>> = HashMap::from([
            (
                "local_paimon".into(),
                local.clone() as Arc<dyn LakeletCatalogProvider>,
            ),
            (
                "Other.Catalog".into(),
                other.clone() as Arc<dyn LakeletCatalogProvider>,
            ),
        ]);
        let session = ExtendedSessionContext::new(
            Arc::new(LakeletCatalogProviderList::from_catalogs(catalogs)),
            Arc::new(LakeletContext::default()),
            Arc::new(RuntimeEnv::default()),
        );
        (session, local, other)
    }

    async fn assert_not_implemented(session: &ExtendedSessionContext, sql: &str) -> Result<()> {
        let error = session.sql(sql).await.expect_err("DDL must fail");
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{sql}: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_ddl_routes_current_session_and_preserves_definition() -> Result<()> {
        let (session, local, other) = recording_session();
        session.sql("USE CATALOG local_paimon").await?;
        assert_not_implemented(&session, "CREATE SCHEMA test").await?;
        assert_not_implemented(&session, "DROP SCHEMA test").await?;
        assert_eq!(local.calls.lock().unwrap().len(), 2);
        local.calls.lock().unwrap().clear();
        session.sql("USE local_paimon.sales").await?;
        // Queries with no table references replace the synchronous catalog list
        // with an empty list. DDL must still find the shared catalog provider.
        session.sql("SELECT 1").await?.collect().await?;

        for (sql, catalog, schema, table) in [
            (
                "CREATE TABLE Orders (id INT)",
                "local_paimon",
                "sales",
                Some("orders"),
            ),
            (
                "CREATE TABLE db.orders (id INT)",
                "local_paimon",
                "db",
                Some("orders"),
            ),
            (
                "CREATE TABLE `Other.Catalog`.`Db.Name`.`Table.Name` (id INT)",
                "Other.Catalog",
                "Db.Name",
                Some("Table.Name"),
            ),
            (
                "CREATE TABLE IF NOT EXISTS orders (id INT) TBLPROPERTIES ('key' = 'value')",
                "local_paimon",
                "sales",
                Some("orders"),
            ),
            ("CREATE SCHEMA Test", "local_paimon", "test", None),
            (
                "CREATE SCHEMA IF NOT EXISTS local_paimon.test",
                "local_paimon",
                "test",
                None,
            ),
            (
                "CREATE SCHEMA `Other.Catalog`.`New.Schema`",
                "Other.Catalog",
                "New.Schema",
                None,
            ),
            (
                "CREATE SCHEMA test AUTHORIZATION owner",
                "local_paimon",
                "test",
                None,
            ),
            (
                "CREATE SCHEMA AUTHORIZATION owner",
                "local_paimon",
                "owner",
                None,
            ),
            ("DROP TABLE Orders", "local_paimon", "sales", Some("orders")),
            ("DROP TABLE db.orders", "local_paimon", "db", Some("orders")),
            (
                "DROP TABLE IF EXISTS `Other.Catalog`.`Db.Name`.`Table.Name` CASCADE",
                "Other.Catalog",
                "Db.Name",
                Some("Table.Name"),
            ),
            (
                "DROP TABLE IF EXISTS orders PURGE",
                "local_paimon",
                "sales",
                Some("orders"),
            ),
            ("DROP SCHEMA Test", "local_paimon", "test", None),
            (
                "DROP SCHEMA IF EXISTS local_paimon.test CASCADE",
                "local_paimon",
                "test",
                None,
            ),
            (
                "DROP SCHEMA `Other.Catalog`.`New.Schema` RESTRICT",
                "Other.Catalog",
                "New.Schema",
                None,
            ),
        ] {
            assert_not_implemented(&session, sql).await?;
            let ExtendedStatement::SQLStatement(stmt) =
                ExtendedParser::parse_sql(sql)?.pop_front().unwrap();
            let expected = match (*stmt, table) {
                (Statement::CreateTable(stmt), Some(table)) => DdlCall::Table(
                    ResolvedTableReference {
                        catalog: catalog.into(),
                        schema: schema.into(),
                        table: table.into(),
                    },
                    Box::new(stmt),
                ),
                (stmt @ Statement::CreateSchema { .. }, None) => DdlCall::Schema(
                    SchemaReference::Full {
                        catalog: catalog.into(),
                        schema: schema.into(),
                    },
                    Box::new(stmt),
                ),
                (
                    stmt @ Statement::Drop {
                        object_type: ObjectType::Table,
                        ..
                    },
                    Some(table),
                ) => DdlCall::DropTable(
                    ResolvedTableReference {
                        catalog: catalog.into(),
                        schema: schema.into(),
                        table: table.into(),
                    },
                    Box::new(stmt),
                ),
                (
                    stmt @ Statement::Drop {
                        object_type: ObjectType::Schema,
                        ..
                    },
                    None,
                ) => DdlCall::DropSchema(
                    SchemaReference::Full {
                        catalog: catalog.into(),
                        schema: schema.into(),
                    },
                    Box::new(stmt),
                ),
                _ => panic!("unexpected test statement"),
            };
            let (target, untouched) = if catalog == "local_paimon" {
                (&local, &other)
            } else {
                (&other, &local)
            };
            assert_eq!(*target.calls.lock().unwrap(), vec![expected], "{sql}");
            assert!(untouched.calls.lock().unwrap().is_empty());
            target.calls.lock().unwrap().clear();
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_ddl_respects_identifier_normalization_setting() -> Result<()> {
        let (session, local, _) = recording_session();
        session.sql("USE local_paimon.sales").await?;
        session
            .sql("SET datafusion.sql_parser.enable_ident_normalization = false")
            .await?;
        assert_not_implemented(&session, "CREATE TABLE Orders (id INT)").await?;
        assert_not_implemented(&session, "CREATE SCHEMA Sales").await?;
        assert_not_implemented(&session, "DROP TABLE Orders").await?;
        assert_not_implemented(&session, "DROP SCHEMA Sales").await?;
        let calls = local.calls.lock().unwrap();
        assert!(matches!(&calls[0], DdlCall::Table(name, _) if name.table.as_ref() == "Orders"));
        assert!(matches!(&calls[1], DdlCall::Schema(name, _) if name.schema_name() == "Sales"));
        assert!(
            matches!(&calls[2], DdlCall::DropTable(name, _) if name.table.as_ref() == "Orders")
        );
        assert!(matches!(&calls[3], DdlCall::DropSchema(name, _) if name.schema_name() == "Sales"));
        Ok(())
    }

    #[tokio::test]
    async fn test_ddl_rejects_invalid_destinations_before_provider_call() -> Result<()> {
        let (session, local, other) = recording_session();
        session.sql("USE local_paimon.sales").await?;
        for sql in [
            "CREATE TABLE missing.sales.t (id INT)",
            "CREATE SCHEMA missing.test",
            "CREATE TABLE a.b.c.d (id INT)",
            "CREATE SCHEMA a.b.c",
            "DROP TABLE IF EXISTS missing.sales.t",
            "DROP SCHEMA IF EXISTS missing.test",
            "DROP TABLE a.b.c.d",
            "DROP SCHEMA a.b.c",
        ] {
            let error = session
                .sql(sql)
                .await
                .expect_err("invalid destination must fail");
            assert!(matches!(error, DataFusionError::Plan(_)), "{sql}: {error}");
        }
        // Preserve the single-object boundary without invoking any provider.
        for sql in ["DROP TABLE t1, t2", "DROP SCHEMA s1, s2"] {
            assert_not_implemented(&session, sql).await?;
        }
        assert!(local.calls.lock().unwrap().is_empty());
        assert!(other.calls.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_ddl_default_provider_returns_not_implemented() -> Result<()> {
        let session = ExtendedSessionContext::default();
        session.sql("USE CATALOG internal").await?;
        for (sql, target) in [
            ("CREATE SCHEMA test", "internal.test"),
            (
                "CREATE TABLE t (id INT) USING PAIMON",
                "internal.information_schema.t",
            ),
            (
                "CREATE TABLE IF NOT EXISTS t USING PAIMON AS SELECT 1 AS id",
                "internal.information_schema.t",
            ),
            ("DROP TABLE IF EXISTS t", "internal.information_schema.t"),
            ("DROP SCHEMA IF EXISTS test CASCADE", "internal.test"),
        ] {
            let error = session.sql(sql).await.expect_err("DDL must fail");
            assert!(
                matches!(error, DataFusionError::NotImplemented(_)),
                "{error}"
            );
            assert!(error.to_string().contains(target), "{error}");
        }
        session.sql("SHOW VARIABLES").await?.collect().await?;
        Ok(())
    }
}
