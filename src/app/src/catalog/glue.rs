use crate::table_format::hive::parse_hive_num_rows;
use crate::catalog::glue_statistics::load_glue_columns_statistics;
use crate::catalog::{CatalogConfig, LakeletCatalogProvider};
use crate::context::LakeletContext;
use crate::table_format::TableFormat;
use crate::table_format::hive::GlueTableSchemaBuilder;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::table_provider_factory::{
    TableProviderBuilder, deduce_table_format, parse_table_reference,
};
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_glue::Client;
use aws_sdk_glue::config::Credentials;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{Result, Statistics, TableReference};
use datafusion::error::DataFusionError;
use lakelet_storage::storage::Storage;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::OnceCell;
use crate::catalog::table_format::hive::parse_hive_total_byte_size;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlueCatalogConfig {
    pub name: String,
    #[serde(rename = "aws-glue-region")]
    pub aws_glue_region: Option<String>,
    #[serde(rename = "aws-glue-access-key")]
    pub aws_glue_access_key: Option<String>,
    #[serde(rename = "aws-glue-secret-key")]
    pub aws_glue_secret_key: Option<String>,
    #[serde(rename = "aws-glue-endpoint")]
    pub aws_glue_endpoint: Option<String>,
    #[serde(flatten, default)]
    pub storage: Storage,
}

pub struct GlueCatalog {
    lakelet_context: Arc<LakeletContext>,
    config: Arc<GlueCatalogConfig>,
    // Lazily built once; the catalog provider list caches GlueCatalog itself,
    // so the client (and its connection pool) outlives a single statement.
    client: OnceCell<Client>,
}

impl GlueCatalog {
    pub fn new(lakelet_context: Arc<LakeletContext>, config: Arc<GlueCatalogConfig>) -> Self {
        Self {
            lakelet_context,
            config,
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Client {
        self.client
            .get_or_init(|| self.build_glue_client())
            .await
            // The aws sdk Client is Arc-backed; clones share the connection pool.
            .clone()
    }

    async fn build_glue_client(&self) -> Client {
        let config = &self.config;
        let mut aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let (Some(access_key), Some(secret_key)) =
            (&config.aws_glue_access_key, &config.aws_glue_secret_key)
        {
            let credential_provider =
                Credentials::new(access_key, secret_key, None, None, "Lakelet");
            aws_config = aws_config.credentials_provider(credential_provider);
        }
        if let Some(region) = &config.aws_glue_region {
            aws_config = aws_config.region(Region::new(region.clone()));
        }
        if let Some(endpoint) = &config.aws_glue_endpoint {
            aws_config = aws_config.endpoint_url(endpoint.clone());
        }
        let aws_config = aws_config.load().await;
        Client::new(&aws_config)
    }
}

#[async_trait]
impl AsyncCatalogProvider for GlueCatalog {
    async fn schema(&self, schema_name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        Ok(Some(Arc::new(GlueSchema::new(
            self.client().await,
            self.lakelet_context.clone(),
            self.config.clone(),
            schema_name.to_string(),
        ))))
    }
}

pub struct GlueSchema {
    glue_client: Client,
    lakelet_context: Arc<LakeletContext>,
    config: Arc<GlueCatalogConfig>,
    schema_name: String,
}

impl GlueSchema {
    pub fn new(
        glue_client: Client,
        lakelet_context: Arc<LakeletContext>,
        config: Arc<GlueCatalogConfig>,
        schema_name: String,
    ) -> Self {
        Self {
            glue_client,
            lakelet_context,
            config,
            schema_name,
        }
    }
}

#[async_trait]
impl AsyncSchemaProvider for GlueSchema {
    async fn table(&self, tbl_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let (table_name, metadata_table_type) = parse_table_reference(tbl_name)?;

        let resp = match self
            .glue_client
            .get_table()
            .database_name(&self.schema_name)
            .name(table_name.as_str())
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|err| err.is_entity_not_found_exception()) =>
            {
                return Ok(None);
            }
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        };

        let glue_table = match resp.table {
            Some(glue_table) => glue_table,
            None => return Ok(None),
        };

        let table_reference = TableReference::full(
            self.config.name.as_str(),
            self.schema_name.as_str(),
            glue_table.name.as_str(),
        );
        let glue_table_properties = match &glue_table.parameters {
            Some(parameters) => parameters.clone(),
            None => {
                return Err(DataFusionError::Internal(
                    "glue table's parameters are missing".to_string(),
                ));
            }
        };

        let storage_descriptor = glue_table.storage_descriptor.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        // TODO: For Paimon Hive catalogs with location-in-properties=true, derive the
        // table location from table properties before falling back to sd.location.
        let table_location = storage_descriptor
            .location()
            .map(ToString::to_string)
            .ok_or_else(|| DataFusionError::Internal("location not existed".to_string()))?;
        let table_format = deduce_table_format(&glue_table_properties)?;
        let (hive_storage_info, hive_partitions) = if table_format == TableFormat::Hive {
            let table_schema = GlueTableSchemaBuilder::new(&glue_table).build()?;
            let mut table_statistics = Statistics::new_unknown(table_schema.table_schema());
            if metadata_table_type.is_none() {
                if let Some(num_rows) = parse_hive_num_rows(&glue_table_properties)
                {
                    table_statistics.num_rows = Precision::Inexact(num_rows);
                }
                if let Some(total_byte_size) = parse_hive_total_byte_size(&glue_table_properties)
                {
                    table_statistics.total_byte_size = Precision::Inexact(total_byte_size);
                }
                // start to fetch column statistics
                match load_glue_columns_statistics(
                    &self.glue_client,
                    &self.schema_name,
                    table_name.as_str(),
                    &table_schema,
                )
                .await
                {
                    Ok(column_statistics) => table_statistics.column_statistics = column_statistics,
                    Err(error) => eprintln!(
                        "Warning: failed to load Glue column statistics for {}.{}: {}",
                        self.schema_name, table_name, error
                    ),
                }
            }
            let hive_storage_info = HiveStorageInfo::try_new_from_glue_table(
                &glue_table,
                table_schema,
                table_statistics,
            )?;
            let hive_partitions = if !hive_storage_info
                .table_schema
                .table_partition_cols()
                .is_empty()
            {
                let paginator = self
                    .glue_client
                    .get_partitions()
                    .database_name(&self.schema_name)
                    .table_name(table_name.as_str())
                    .into_paginator()
                    .send();
                tokio::pin!(paginator);

                let mut partitions = Vec::new();
                while let Some(page) = paginator.next().await {
                    let page = page.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    partitions.extend(
                        page.partitions()
                            .iter()
                            .map(HivePartition::try_new_from_glue_partition)
                            .collect::<Result<Vec<_>>>()?,
                    );
                }
                partitions
            } else {
                vec![]
            };
            (Some(hive_storage_info), Some(hive_partitions))
        } else {
            (None, None)
        };

        let table_provider_builder = TableProviderBuilder::new(
            self.lakelet_context.clone(),
            table_location,
            table_reference,
            glue_table_properties,
            table_format,
            CatalogConfig::GLUE(self.config.deref().clone()),
        );
        let table_provider_builder = table_provider_builder
            .with_metadata_table_type(metadata_table_type)
            .with_hive_storage_info(hive_storage_info)
            .with_hive_partitions(hive_partitions);

        Ok(Some(table_provider_builder.build().await?))
    }
}

#[async_trait]
impl LakeletCatalogProvider for GlueCatalog {
    async fn list_schema_names(&self) -> Result<Vec<String>> {
        let glue_client = self.client().await;
        let paginator = glue_client.get_databases().into_paginator().send();
        tokio::pin!(paginator);

        let mut schema_names = Vec::new();
        while let Some(page) = paginator.next().await {
            let page = page.map_err(|e| DataFusionError::External(Box::new(e)))?;
            for database in page.database_list() {
                schema_names.push(database.name.clone());
            }
        }

        Ok(schema_names)
    }

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let glue_client = self.client().await;
        let paginator = glue_client
            .get_tables()
            .database_name(schema_name)
            .into_paginator()
            .send();
        tokio::pin!(paginator);

        let mut table_names = Vec::new();
        while let Some(page) = paginator.next().await {
            let page = page.map_err(|e| DataFusionError::External(Box::new(e)))?;
            for table in page.table_list() {
                table_names.push(table.name.clone());
            }
        }

        Ok(table_names)
    }

    async fn schema_exist(&self, schema_name: &str) -> Result<bool> {
        let glue_client = self.client().await;
        match glue_client.get_database().name(schema_name).send().await {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|err| err.is_entity_not_found_exception()) =>
            {
                Ok(false)
            }
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool> {
        let glue_client = self.client().await;
        match glue_client
            .get_table()
            .database_name(schema_name)
            .name(table_name)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|err| err.is_entity_not_found_exception()) =>
            {
                Ok(false)
            }
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_glue_catalog_config() {
        let config: GlueCatalogConfig = toml::from_str(
            r#"
            name = "glue_local"
            aws-glue-region = "us-east-1"
            aws-glue-access-key = "access-key"
            aws-glue-secret-key = "secret-key"
            aws-glue-endpoint = "http://127.0.0.1:4566"
            "#,
        )
        .unwrap();

        assert_eq!(config.name, "glue_local");
        assert_eq!(config.aws_glue_region.as_deref(), Some("us-east-1"));
        assert_eq!(config.aws_glue_access_key.as_deref(), Some("access-key"));
        assert_eq!(config.aws_glue_secret_key.as_deref(), Some("secret-key"));
        assert_eq!(
            config.aws_glue_endpoint.as_deref(),
            Some("http://127.0.0.1:4566")
        );
    }
}
