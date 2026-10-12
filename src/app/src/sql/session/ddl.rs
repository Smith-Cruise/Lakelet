use super::ExtendedSessionContext;
use crate::catalog::CreateTableRequest;
use datafusion::common::{
    ResolvedTableReference, Result, SchemaReference, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::sqlparser::ast::{
    CreateTable, HiveIOFormat, ObjectName, ObjectType, SchemaName, Statement,
};
use datafusion::sql::planner::{IdentNormalizer, object_name_to_table_reference};

impl ExtendedSessionContext {
    fn resolve_ddl_table_name(&self, name: &ObjectName) -> Result<ResolvedTableReference> {
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

        // Creation targets are not query inputs. Let the catalog handle the DDL
        // before resolving any tables or planning a possible AS SELECT query.
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
        provider
            .create_table(&CreateTableRequest {
                name,
                table_format,
                definition: stmt,
                enable_ident_normalization,
            })
            .await?;
        self.session_context
            .execute_logical_plan(LogicalPlanBuilder::empty(false).build()?)
            .await
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

        async fn create_table(&self, request: &CreateTableRequest<'_>) -> Result<()> {
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
            (
                "CREATE TABLE orders AS SELECT * FROM missing.schema.source",
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
                "CREATE TABLE IF NOT EXISTS t USING PAIMON AS SELECT * FROM missing",
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
