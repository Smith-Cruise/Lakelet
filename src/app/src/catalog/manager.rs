use crate::catalog::iceberg_rest::IcebergRestCatalogConfig;
use crate::glue_catalog::GlueCatalogConfig;
use crate::hms_catalog::HMSCatalogConfig;
use crate::internal_catalog::INTERNAL_CATALOG;
use crate::paimon_fs_catalog::PaimonFSCatalogConfig;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Serialize, Deserialize)]
pub struct CatalogConfigs {
    #[serde(rename = "iceberg-rest")]
    pub iceberg_rest: Option<Vec<IcebergRestCatalogConfig>>,
    pub hms: Option<Vec<HMSCatalogConfig>>,
    pub glue: Option<Vec<GlueCatalogConfig>>,
    #[serde(rename = "paimon-fs")]
    pub paimon_fs: Option<Vec<PaimonFSCatalogConfig>>,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Internal,
    IcebergRest(IcebergRestCatalogConfig),
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
    PaimonFS(PaimonFSCatalogConfig),
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

        if let Some(ref paimon_fs_catalogs) = catalogs.paimon_fs {
            for paimon_fs_catalog in paimon_fs_catalogs {
                self.add_catalog(
                    &paimon_fs_catalog.name,
                    CatalogConfig::PaimonFS(paimon_fs_catalog.clone()),
                )?;
            }
        }

        if let Some(rest_catalogs) = &catalogs.iceberg_rest {
            for config in rest_catalogs {
                self.add_catalog(&config.name, CatalogConfig::IcebergRest(config.clone()))?;
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

    pub fn catalog_exists(&self, catalog_name: &str) -> bool {
        self.catalogs.contains_key(catalog_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_format_catalogs_from_config() {
        let configs: CatalogConfigs = toml::from_str(
            r#"
            [[paimon-fs]]
            name = "paimon_fs_1"
            warehouse = "s3://bucket/warehouse"

            [[iceberg-rest]]
            name = "iceberg_prod"
            uri = "http://localhost:8181"
            s3-storage = { region = "us-east-1", access-key = "ak", secret-key = "sk" }
        "#,
        )
        .unwrap();

        let mut catalog_manager = CatalogManager::new();
        catalog_manager.load_catalogs(&configs).unwrap();
        assert!(catalog_manager.catalog_exists("paimon_fs_1"));
        assert!(matches!(
            catalog_manager.get_catalog("paimon_fs_1"),
            Some(CatalogConfig::PaimonFS(_))
        ));
        let Some(CatalogConfig::IcebergRest(rest)) = catalog_manager.get_catalog("iceberg_prod")
        else {
            panic!("iceberg_prod should be an Iceberg REST catalog");
        };
        assert!(rest.storage.s3_storage.is_some());
    }

    #[test]
    fn test_load_catalogs_rejects_duplicate_names() {
        let configs: CatalogConfigs = toml::from_str(
            r#"
            [[hms]]
            name = "dup"
            metastore-uri = "127.0.0.1:9083"

            [[paimon-fs]]
            name = "dup"
            warehouse = "s3://bucket/warehouse"
        "#,
        )
        .unwrap();

        let mut catalog_manager = CatalogManager::new();
        let result = catalog_manager.load_catalogs(&configs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }
}
