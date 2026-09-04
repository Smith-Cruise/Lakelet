mod show;
mod r#use;

use crate::catalog::{INTERNAL_CATALOG, LakeletCatalogProviderList};
use crate::context::LakeletContext;
use crate::parser::ExtendedParser;
use crate::statements::ExtendedStatement;
use datafusion::catalog::AsyncCatalogProviderList;
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::ExplainFormat;
use datafusion::logical_expr::sqlparser::ast::Statement;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_cli::functions::MetadataCacheFunc;
use std::sync::Arc;

pub struct ExtendedSessionContext {
    // Injected by the server entry point and shared across sessions, so
    // catalog providers and their metastore clients are built once per
    // process instead of per statement.
    catalog_provider_list: Arc<LakeletCatalogProviderList>,
    lakelet_context: Arc<LakeletContext>,
    session_context: SessionContext,
}

impl Default for ExtendedSessionContext {
    fn default() -> Self {
        let lakelet_context = Arc::new(LakeletContext::default());
        let runtime_env = Arc::new(RuntimeEnv::default());
        let catalog_provider_list =
            Arc::new(LakeletCatalogProviderList::new(lakelet_context.clone()));
        Self::new(catalog_provider_list, lakelet_context, runtime_env)
    }
}

impl ExtendedSessionContext {
    pub fn new(
        catalog_provider_list: Arc<LakeletCatalogProviderList>,
        lakelet_context: Arc<LakeletContext>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Self {
        let catalog = lakelet_context
            .default_catalog
            .as_deref()
            .unwrap_or(INTERNAL_CATALOG);
        let schema = lakelet_context
            .default_schema
            .as_deref()
            .unwrap_or(INFORMATION_SCHEMA);
        let mut options = ConfigOptions::new();
        options.explain.format = ExplainFormat::Tree;
        let session_config =
            SessionConfig::from(options).with_default_catalog_and_schema(catalog, schema);
        let session_context = SessionContext::new_with_config_rt(session_config, runtime_env);
        // `SELECT * FROM metadata_cache()` lists the parquet footers held in the
        // process-wide file metadata cache (path, size, hits), the same table
        // function datafusion-cli ships.
        session_context.register_udtf(
            "metadata_cache",
            Arc::new(MetadataCacheFunc::new(
                session_context.runtime_env().cache_manager.clone(),
            )),
        );
        Self {
            catalog_provider_list,
            session_context,
            lakelet_context,
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let parser = ExtendedParser::parse_sql(sql)?;
        if parser.len() != 1 {
            // A client error, not an execution failure: callers map `Plan` to
            // 400 / INVALID_ARGUMENT rather than a generic internal error.
            return Err(DataFusionError::Plan(format!(
                "Expected exactly one SQL statement, got {}: {}",
                parser.len(),
                sql
            )));
        }

        let stmt = &parser[0];
        self.create_dataframe(stmt).await
    }

    pub async fn create_dataframe(&self, statement: &ExtendedStatement) -> Result<DataFrame> {
        let sql_string: String = match statement {
            ExtendedStatement::ShowCatalogsStatement(show_catalogs) => {
                return self.handle_show_catalogs(show_catalogs);
            }
            ExtendedStatement::ShowVariablesStatement(show_variables) => {
                self.build_show_variables_sql(&show_variables.filter, show_variables.verbose)?
            }
            ExtendedStatement::SQLStatement(stmt) => match stmt.as_ref() {
                Statement::Use(use_stmt) => {
                    return self.handle_use_stmt(use_stmt).await;
                }
                Statement::ShowSchemas { show_options, .. } => {
                    return self.handle_show_schemas(show_options).await;
                }
                Statement::ShowTables { show_options, .. } => {
                    return self.handle_show_tables(show_options).await;
                }
                _ => stmt.to_string(),
            },
        };

        // Instead, to use a remote catalog, we must use lower level APIs on
        // SessionState (what `SessionContext::sql` does internally).
        let state = self.session_context.state();
        // First, parse the SQL (but don't plan it / resolve any table references)
        let dialect = state.config().options().sql_parser.dialect;
        let statement = state.sql_to_statement(&sql_string, &dialect)?;
        // Find all `TableReferences` in the parsed queries. These correspond to the
        // tables referred to by the query (in this case
        // `remote_schema.remote_table`)

        // DataFusion resolves SHOW CREATE through information_schema internally,
        // which adds synthetic references such as information_schema.columns.
        // Remote catalogs should only resolve the target table here; otherwise
        // HMS/Glue would try to load those synthetic information_schema tables.
        let references = match Self::show_create_statement(&statement) {
            Some((obj_type, obj_name)) => {
                self.resolve_show_create_table_references(obj_type, obj_name)?
            }
            None => state.resolve_table_references(&statement)?,
        };

        // Now we can asynchronously resolve the table references to get a cached catalog
        // that we can use for our query. The provider list is shared across
        // statements, so catalog providers and their metastore clients are
        // reused instead of rebuilt per statement.
        let resolved_catalog_providers = self
            .catalog_provider_list
            .resolve(&references, state.config())
            .await?;
        self.session_context
            .register_catalog_list(resolved_catalog_providers);
        if let Some((obj_type, obj_name)) = Self::show_create_statement(&statement) {
            return self.handle_show_create_stmt(obj_type, obj_name).await;
        }
        self.session_context.sql(&sql_string).await
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.session_context.task_ctx()
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.session_context
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn metadata_cache_table_function_is_registered() -> Result<()> {
        let ctx = ExtendedSessionContext::default();
        let df = ctx.sql("SELECT path, hits FROM metadata_cache()").await?;
        let batches = df.collect().await?;
        // Nothing has been scanned yet, so the cache is empty; the query itself
        // must plan and run.
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        Ok(())
    }
}
