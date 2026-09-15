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
use datafusion::common::Result;
pub(crate) use internal::{INFORMATION_SCHEMA_SHOW_VARIABLES, INTERNAL_CATALOG};
pub(crate) use manager::{CatalogConfig, CatalogConfigs, CatalogManager};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
pub(crate) use table_definition_builder::TableDefinitionBuilder;

#[async_trait]
pub trait LakeletCatalogProvider: AsyncCatalogProvider {
    async fn list_schema_names(&self) -> Result<Vec<String>>;

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>>;

    async fn schema_exist(&self, schema_name: &str) -> Result<bool>;

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool>;
}

pub struct LakeletCatalogProviderList {
    lakelet_context: Arc<LakeletContext>,
    // One catalog provider per configured catalog, built on first reference.
    // Providers own their metastore clients, so caching them here keeps
    // clients (and their connection pools) alive across statements.
    catalogs: Mutex<HashMap<String, Arc<dyn LakeletCatalogProvider>>>,
}

impl LakeletCatalogProviderList {
    pub fn new(lakelet_context: Arc<LakeletContext>) -> LakeletCatalogProviderList {
        Self {
            lakelet_context,
            catalogs: Mutex::new(HashMap::new()),
        }
    }

    // Keep this synchronous: every provider constructor is sync, so the lock
    // is never held across an await point.
    pub fn get_catalog(
        &self,
        catalog_name: &str,
    ) -> Result<Option<Arc<dyn LakeletCatalogProvider>>> {
        let mut catalogs = self.catalogs.lock().unwrap();
        if let Some(catalog) = catalogs.get(catalog_name) {
            return Ok(Some(catalog.clone()));
        }

        // start to create catalog
        let catalog_config = if let Some(catalog_config) = self
            .lakelet_context
            .catalog_manager
            .get_catalog(catalog_name)
        {
            catalog_config.clone()
        } else {
            return Ok(None);
        };

        let catalog: Arc<dyn LakeletCatalogProvider> = match catalog_config {
            CatalogConfig::IcebergRest(config) => {
                Arc::new(IcebergRestCatalog::new(Arc::new(config)))
            }
            CatalogConfig::Internal => Arc::new(InternalCatalog::new(self.lakelet_context.clone())),
            CatalogConfig::HMS(hms_catalog) => Arc::new(HMSCatalog::new(
                self.lakelet_context.clone(),
                Arc::new(hms_catalog),
            )),
            CatalogConfig::GLUE(glue_catalog) => Arc::new(GlueCatalog::new(
                self.lakelet_context.clone(),
                Arc::new(glue_catalog),
            )),
            CatalogConfig::PaimonFS(paimon_fs_catalog) => Arc::new(PaimonFSCatalog::try_new(
                self.lakelet_context.clone(),
                Arc::new(paimon_fs_catalog),
            )?),
        };
        catalogs.insert(catalog_name.to_string(), catalog.clone());
        Ok(Some(catalog))
    }
}

#[async_trait]
impl AsyncCatalogProviderList for LakeletCatalogProviderList {
    async fn catalog(&self, catalog_name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        let Some(catalog) = self.get_catalog(catalog_name)? else {
            return Ok(None);
        };
        Ok(Some(catalog))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_catalog_provider_list_caches_providers() {
        let provider_list = LakeletCatalogProviderList::new(Arc::new(LakeletContext::default()));
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
        // Repeated resolution returns the same cached provider instance.
        assert!(Arc::ptr_eq(&first, &second));
        assert!(provider_list.catalog("missing").await.unwrap().is_none());
    }
}
