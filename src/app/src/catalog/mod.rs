pub(crate) mod data_file_format;
pub(crate) mod glue;
pub(crate) mod hms;
pub(crate) mod iceberg_rest;
pub(crate) mod internal;
pub(crate) mod manager;
pub(crate) mod paimon_fs;
pub(crate) mod statistics;
pub(crate) mod table_definition_builder;
pub(crate) mod table_format;

use crate::LakeletContext;
use crate::catalog::glue::GlueCatalog;
use crate::catalog::hms::HMSCatalog;
use crate::catalog::iceberg_rest::IcebergRestCatalog;
use crate::catalog::internal::InternalCatalog;
use crate::catalog::paimon_fs::PaimonFSCatalog;
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList};
use datafusion::common::{ResolvedTableReference, Result, SchemaReference, not_impl_err};
use datafusion::logical_expr::sqlparser::ast::{CreateTable, Statement};
pub(crate) use internal::{INFORMATION_SCHEMA_SHOW_VARIABLES, INTERNAL_CATALOG};
pub(crate) use manager::{CatalogConfig, CatalogConfigs, CatalogManager};
use std::collections::HashMap;
use std::sync::Arc;
pub(crate) use table_definition_builder::TableDefinitionBuilder;
use table_format::TableFormat;

/// A catalog-independent CREATE TABLE request retaining the SQL type information.
pub struct CreateTableRequest<'a> {
    pub name: ResolvedTableReference,
    pub table_format: TableFormat,
    pub definition: &'a CreateTable,
    pub enable_ident_normalization: bool,
}

#[async_trait]
pub trait LakeletCatalogProvider: AsyncCatalogProvider {
    async fn list_schema_names(&self) -> Result<Vec<String>>;

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>>;

    async fn schema_exist(&self, schema_name: &str) -> Result<bool>;

    fn default_table_format(&self) -> Option<TableFormat> {
        None
    }

    /// Create a persistent table using the resolved destination and requested format.
    async fn create_table(&self, request: &CreateTableRequest<'_>) -> Result<()> {
        not_impl_err!(
            "CREATE TABLE {} USING {:?} is not supported",
            request.name,
            request.table_format
        )
    }

    /// Create a schema at the fully resolved destination using the original SQL definition.
    async fn create_schema(&self, name: &SchemaReference, _stmt: &Statement) -> Result<()> {
        not_impl_err!("CREATE SCHEMA {name} is not supported")
    }

    /// Drop a table at the fully resolved destination using the original SQL options.
    async fn drop_table(&self, name: &ResolvedTableReference, _stmt: &Statement) -> Result<()> {
        not_impl_err!("DROP TABLE {name} is not supported")
    }

    /// Drop a schema at the fully resolved destination using the original SQL options.
    async fn drop_schema(&self, name: &SchemaReference, _stmt: &Statement) -> Result<()> {
        not_impl_err!("DROP SCHEMA {name} is not supported")
    }
}

pub struct LakeletCatalogProviderList {
    // Every configured catalog gets a provider up front, so this map is
    // immutable afterwards and needs no lock. Providers build their metastore
    // clients lazily, so registering them all stays cheap.
    catalogs: HashMap<String, Arc<dyn LakeletCatalogProvider>>,
}

impl LakeletCatalogProviderList {
    #[cfg(test)]
    pub(crate) fn from_catalogs(
        catalogs: HashMap<String, Arc<dyn LakeletCatalogProvider>>,
    ) -> Self {
        Self { catalogs }
    }

    pub fn new(lakelet_context: Arc<LakeletContext>) -> Result<LakeletCatalogProviderList> {
        let mut catalogs = HashMap::new();
        for (catalog_name, catalog_config) in lakelet_context.catalog_manager.list_catalogs() {
            let catalog = build_catalog_provider(&lakelet_context, catalog_config)?;
            catalogs.insert(catalog_name, catalog);
        }
        Ok(Self { catalogs })
    }

    pub fn get_catalog(&self, catalog_name: &str) -> Option<Arc<dyn LakeletCatalogProvider>> {
        self.catalogs.get(catalog_name).cloned()
    }

    /// The names of every configured catalog, in no particular order.
    pub fn catalog_names(&self) -> Vec<String> {
        self.catalogs.keys().cloned().collect()
    }
}

fn build_catalog_provider(
    lakelet_context: &Arc<LakeletContext>,
    catalog_config: CatalogConfig,
) -> Result<Arc<dyn LakeletCatalogProvider>> {
    let catalog: Arc<dyn LakeletCatalogProvider> = match catalog_config {
        CatalogConfig::IcebergRest(config) => Arc::new(IcebergRestCatalog::new(Arc::new(config))),
        CatalogConfig::Internal => Arc::new(InternalCatalog::new(lakelet_context.clone())),
        CatalogConfig::HMS(hms_catalog) => Arc::new(HMSCatalog::new(
            lakelet_context.clone(),
            Arc::new(hms_catalog),
        )),
        CatalogConfig::GLUE(glue_catalog) => Arc::new(GlueCatalog::new(
            lakelet_context.clone(),
            Arc::new(glue_catalog),
        )),
        CatalogConfig::PaimonFS(paimon_fs_catalog) => Arc::new(PaimonFSCatalog::try_new(
            lakelet_context.clone(),
            Arc::new(paimon_fs_catalog),
        )?),
    };
    Ok(catalog)
}

#[async_trait]
impl AsyncCatalogProviderList for LakeletCatalogProviderList {
    async fn catalog(&self, catalog_name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        let Some(catalog) = self.get_catalog(catalog_name) else {
            return Ok(None);
        };
        Ok(Some(catalog))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_catalog_provider_list_registers_configured_catalogs() {
        let provider_list =
            LakeletCatalogProviderList::new(Arc::new(LakeletContext::default())).unwrap();
        let first = provider_list
            .catalog(INTERNAL_CATALOG)
            .await
            .unwrap()
            .unwrap();
        let second = provider_list
            .catalog(INTERNAL_CATALOG)
            .await
            .unwrap()
            .unwrap();
        // Repeated resolution returns the same registered provider instance.
        assert!(Arc::ptr_eq(&first, &second));
        assert!(provider_list.catalog("missing").await.unwrap().is_none());
    }
}
