mod delta_table_provider;

use crate::catalog::table_format::metadata_table::MetadataTableType;
use crate::table_format::delta::delta_table_provider::DeltaTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use lakelet_storage::storage::Storage;
use std::sync::Arc;

pub struct DeltaTableProviderFactory {}

impl DeltaTableProviderFactory {
    pub async fn try_create_table_provider(
        table_reference: TableReference,
        table_location: String,
        metadata_table_type: Option<MetadataTableType>,
        storage: Storage,
    ) -> Result<Arc<dyn TableProvider>> {
        if let Some(metadata_table_type) = metadata_table_type {
            return Err(DataFusionError::NotImplemented(format!(
                "delta metadata table {metadata_table_type:?} is not supported"
            )));
        }
        let table_provider =
            DeltaTableProvider::try_new(table_reference, table_location, storage).await?;
        Ok(Arc::new(table_provider))
    }
}
