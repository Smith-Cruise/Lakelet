use crate::context::LakeletContext;
use crate::glue_catalog::{GlueCatalog, GlueCatalogConfig};
use crate::hms_catalog::{HMSCatalog, HMSCatalogConfig};
use crate::internal_catalog::{INTERNAL_CATALOG, InternalCatalog};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use lakelet_common::runtime::RuntimeManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Serialize, Deserialize)]
pub struct CatalogConfigs {
    pub hms: Option<Vec<HMSCatalogConfig>>,
    pub glue: Option<Vec<GlueCatalogConfig>>,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Internal,
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
}

#[derive(Debug, Clone)]
pub struct CatalogManager {
    catalogs: HashMap<String, CatalogConfig>,
}

impl Default for CatalogManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogManager {
    pub fn new() -> Self {
        let mut catalogs = HashMap::new();
        catalogs.insert(INTERNAL_CATALOG.to_string(), CatalogConfig::Internal);
        Self { catalogs }
    }
    fn add_catalog(&mut self, catalog_name: &str, catalog_config: CatalogConfig) -> Result<()> {
        if self.catalogs.contains_key(catalog_name) {
            Err(DataFusionError::Configuration(format!(
                "Catalog {} already exists",
                catalog_name
            )))
        } else {
            self.catalogs
                .insert(catalog_name.to_string(), catalog_config);
            Ok(())
        }
    }

    pub fn load_catalogs(&mut self, catalogs: &CatalogConfigs) -> Result<()> {
        if let Some(ref hms_catalogs) = catalogs.hms {
            for hms_catalog in hms_catalogs {
                self.add_catalog(&hms_catalog.name, CatalogConfig::HMS(hms_catalog.clone()))?;
            }
        }

        if let Some(ref glue_catalogs) = catalogs.glue {
            for glue_catalog in glue_catalogs {
                self.add_catalog(
                    &glue_catalog.name,
                    CatalogConfig::GLUE(glue_catalog.clone()),
                )?;
            }
        }

        Ok(())
    }

    pub fn get_catalog(&self, catalog_name: &str) -> Option<&CatalogConfig> {
        self.catalogs.get(catalog_name)
    }

    pub fn list_catalogs(&self) -> Vec<(String, CatalogConfig)> {
        self.catalogs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn build_catalog_provider(
        &self,
        catalog_name: &str,
    ) -> Result<Box<dyn LakeletCatalogProvider + Send + Sync>> {
        let catalog_config = self
            .get_catalog(catalog_name)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown catalog {}", catalog_name)))?;
        let lakelet_context = Arc::new(LakeletContext {
            server_config: Default::default(),
            catalog_manager: Arc::new(self.clone()),
            runtime_manager: Arc::new(RuntimeManager::default()),
            default_catalog: None,
            default_schema: None,
        });

        match catalog_config {
            CatalogConfig::Internal => Ok(Box::new(InternalCatalog::new(lakelet_context))),
            CatalogConfig::HMS(hms_catalog) => Ok(Box::new(HMSCatalog::new(
                lakelet_context,
                Arc::new(hms_catalog.clone()),
            ))),
            CatalogConfig::GLUE(glue_catalog) => Ok(Box::new(GlueCatalog::new(
                lakelet_context,
                Arc::new(glue_catalog.clone()),
            ))),
        }
    }

    pub fn catalog_exists(&self, catalog_name: &str) -> bool {
        self.catalogs.contains_key(catalog_name)
    }

    pub async fn list_schema_names(&self, catalog_name: &str) -> Result<Vec<String>> {
        self.build_catalog_provider(catalog_name)?
            .list_schema_names()
            .await
    }

    pub async fn list_table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>> {
        self.build_catalog_provider(catalog_name)?
            .list_table_names(schema_name)
            .await
    }

    pub async fn schema_exist(&self, catalog_name: &str, schema_name: &str) -> Result<bool> {
        self.build_catalog_provider(catalog_name)?
            .schema_exist(schema_name)
            .await
    }

    pub async fn table_exists(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool> {
        self.build_catalog_provider(catalog_name)?
            .table_exist(table_name, schema_name)
            .await
    }
}

pub struct LakeletCatalogProviderList {
    lakelet_context: Arc<LakeletContext>,
}

impl LakeletCatalogProviderList {
    pub fn new(lakelet_context: Arc<LakeletContext>) -> LakeletCatalogProviderList {
        Self { lakelet_context }
    }
}

#[async_trait]
impl AsyncCatalogProviderList for LakeletCatalogProviderList {
    async fn catalog(&self, catalog_name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        let catalog_config = if let Some(catalog_config) = self
            .lakelet_context
            .catalog_manager
            .get_catalog(catalog_name)
        {
            catalog_config.clone()
        } else {
            return Ok(None);
        };

        match catalog_config {
            CatalogConfig::Internal => Ok(Some(Arc::new(
                crate::internal_catalog::InternalCatalog::new(self.lakelet_context.clone()),
            ))),
            CatalogConfig::HMS(hms_catalog) => Ok(Some(Arc::new(HMSCatalog::new(
                self.lakelet_context.clone(),
                Arc::new(hms_catalog),
            )))),
            CatalogConfig::GLUE(glue_catalog) => Ok(Some(Arc::new(GlueCatalog::new(
                self.lakelet_context.clone(),
                Arc::new(glue_catalog),
            )))),
        }
    }
}

#[async_trait]
pub trait LakeletCatalogProvider {
    async fn list_schema_names(&self) -> Result<Vec<String>>;

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>>;

    async fn schema_exist(&self, schema_name: &str) -> Result<bool>;

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool>;
}
