mod paimon_table_provider;
mod single_table_catalog;

use crate::table_format::metadata_table::MetadataTableType;
use crate::table_format::paimon::paimon_table_provider::LakeletPaimonTableProvider;
use crate::table_format::paimon::single_table_catalog::SingleTablePaimonCatalog;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result, TableReference};
use lakelet_storage::storage::Storage;
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::table::{SchemaManager, Table};
use paimon_datafusion::{BlobReaderRegistry, PaimonSchemaProvider, PaimonTableProvider};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct PaimonTableProviderFactory;

impl PaimonTableProviderFactory {
    pub async fn try_create_table_provider(
        table_reference: TableReference,
        table_location: String,
        metadata_table_type: Option<MetadataTableType>,
        storage: Storage,
    ) -> Result<Arc<dyn TableProvider>> {
        let file_io_properties = storage.build_paimon_file_io_properties();
        let file_io = build_file_io(&table_location, file_io_properties)?;
        let schema = SchemaManager::new(file_io.clone(), table_location.clone())
            .latest()
            .await
            .map_err(to_datafusion_error)?
            .ok_or_else(|| {
                DataFusionError::External(
                    format!("Paimon schema not found at {table_location}").into(),
                )
            })?;
        let (database, table_name) = table_identifier_parts(&table_reference)?;
        let inner_table = Table::new(
            file_io,
            Identifier::new(database, table_name),
            table_location.clone(),
            (*schema).clone(),
            None,
        );
        if let Some(metadata_table_type) = metadata_table_type {
            return Self::try_create_metadata_table_provider(inner_table, metadata_table_type)
                .await;
        }
        let inner_provider = PaimonTableProvider::try_new(inner_table)?;
        let provider =
            LakeletPaimonTableProvider::try_new(table_reference, table_location, inner_provider)?;
        Ok(Arc::new(provider))
    }

    /// Serves `table$<metadata>` through paimon-datafusion's system tables.
    ///
    /// The system-table builders are private in paimon-datafusion; the only
    /// public entry point is `PaimonSchemaProvider::table`, so we wrap the
    /// already-loaded table in a single-table catalog and resolve the
    /// `<table>$<system>` name through a throwaway schema provider.
    async fn try_create_metadata_table_provider(
        inner_table: Table,
        metadata_table_type: MetadataTableType,
    ) -> Result<Arc<dyn TableProvider>> {
        let system_table_name = paimon_system_table_name(metadata_table_type)?;
        let identifier = inner_table.identifier().clone();
        let schema_provider = PaimonSchemaProvider::new(
            None,
            Arc::new(SingleTablePaimonCatalog::new(inner_table)),
            identifier.database().to_string(),
            Arc::new(RwLock::new(HashMap::new())),
            None,
            BlobReaderRegistry::default(),
            None,
        );
        let system_table_reference = format!("{}${}", identifier.object(), system_table_name);
        schema_provider
            .table(&system_table_reference)
            .await?
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "paimon system table {system_table_reference} was not resolved"
                ))
            })
    }
}

/// Maps Lakelet's metadata table type to the system-table name recognized by
/// paimon-datafusion. Note `DataFiles` maps to `files`: users write
/// `table$data_files` in SQL, consistent with the other formats.
fn paimon_system_table_name(metadata_table_type: MetadataTableType) -> Result<&'static str> {
    match metadata_table_type {
        MetadataTableType::DataFiles => Ok("files"),
        MetadataTableType::Partitions => Ok("partitions"),
        MetadataTableType::Snapshots => Ok("snapshots"),
        MetadataTableType::Manifests => Ok("manifests"),
        MetadataTableType::Options => Ok("options"),
        MetadataTableType::Schemas => Ok("schemas"),
        MetadataTableType::Tags => Ok("tags"),
        MetadataTableType::Branches => Ok("branches"),
        MetadataTableType::PaimonTableIndexes => Ok("table_indexes"),
        MetadataTableType::PaimonPhysicalFilesSize => Ok("physical_files_size"),
        MetadataTableType::PaimonReferencedFilesSize => Ok("referenced_files_size"),
        MetadataTableType::History => Err(DataFusionError::NotImplemented(format!(
            "paimon metadata table {metadata_table_type:?} is not supported"
        ))),
    }
}

fn build_file_io(table_location: &str, properties: HashMap<String, String>) -> Result<FileIO> {
    FileIO::from_path(table_location)
        .and_then(|builder| builder.with_props(properties).build())
        .map_err(to_datafusion_error)
}

fn table_identifier_parts(table_reference: &TableReference) -> Result<(String, String)> {
    match table_reference {
        TableReference::Full { schema, table, .. } => Ok((schema.to_string(), table.to_string())),
        _ => Err(DataFusionError::Plan(
            "Paimon table reference must be fully qualified".to_string(),
        )),
    }
}

fn to_datafusion_error(error: paimon::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
