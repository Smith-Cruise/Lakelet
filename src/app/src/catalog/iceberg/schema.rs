use super::to_datafusion_error;
use crate::table_format::iceberg::iceberg_metadata_table_provider::IcebergMetadataTableProvider;
use crate::table_format::iceberg::iceberg_table_provider::IcebergTableProvider;
use crate::table_format::parse_table_reference;
use async_trait::async_trait;
use datafusion::catalog::{AsyncSchemaProvider, TableProvider};
use datafusion::common::{ResolvedTableReference, Result};
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};
use std::sync::Arc;

pub(super) struct IcebergSchema {
    pub(super) catalog_name: String,
    pub(super) namespace: NamespaceIdent,
    pub(super) client: Arc<dyn Catalog>,
}

#[async_trait]
impl AsyncSchemaProvider for IcebergSchema {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let (base_name, metadata_type) = parse_table_reference(name)?;
        let ident = TableIdent::new(self.namespace.clone(), base_name);
        let table = match self.client.load_table(&ident).await {
            Ok(table) => table,
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(to_datafusion_error(error)),
        };
        if let Some(metadata_type) = metadata_type {
            return Ok(Some(Arc::new(IcebergMetadataTableProvider::try_new(
                table,
                metadata_type,
            )?)));
        }
        let reference = ResolvedTableReference {
            catalog: self.catalog_name.as_str().into(),
            schema: self.namespace.to_string().into(),
            table: ident.name.as_str().into(),
        };
        let location = table.metadata().location().to_string();
        Ok(Some(Arc::new(
            IcebergTableProvider::try_new_from_table(reference, location, table).await?,
        )))
    }
}
