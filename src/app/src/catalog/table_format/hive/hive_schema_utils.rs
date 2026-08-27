use crate::table_format::hive::hive_type::hive_type_to_arrow_type;
use aws_sdk_glue::types::{Column as GlueColumn, Table as GlueTable};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::table_schema::TableSchema;
use hive_metastore::{FieldSchema, Table as HMSTable};
use std::sync::Arc;

pub(crate) struct GlueTableSchemaBuilder<'a> {
    table: &'a GlueTable,
}

impl<'a> GlueTableSchemaBuilder<'a> {
    pub(crate) fn new(table: &'a GlueTable) -> Self {
        Self { table }
    }

    pub(crate) fn build(&self) -> Result<TableSchema> {
        let storage_descriptor = self.table.storage_descriptor.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        let data_cols = Self::build_fields(storage_descriptor.columns())?;
        let table_partition_cols = Self::build_fields(self.table.partition_keys())?;
        Ok(TableSchema::new(
            Arc::new(Schema::new(data_cols)),
            table_partition_cols,
        ))
    }

    fn build_fields(columns: &[GlueColumn]) -> Result<Vec<Arc<Field>>> {
        columns
            .iter()
            .map(|column| {
                let data_type = column.r#type().ok_or_else(|| {
                    DataFusionError::Internal("FieldSchema's type not existed".to_string())
                })?;
                build_field(column.name(), data_type)
            })
            .collect()
    }
}

pub(crate) struct HMSTableSchemaBuilder<'a> {
    table: &'a HMSTable,
}

impl<'a> HMSTableSchemaBuilder<'a> {
    pub(crate) fn new(table: &'a HMSTable) -> Self {
        Self { table }
    }

    pub(crate) fn build(&self) -> Result<TableSchema> {
        let storage_descriptor = self.table.sd.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        let data_cols = Self::build_fields(&storage_descriptor.cols)?;
        let table_partition_cols = Self::build_fields(&self.table.partition_keys)?;
        Ok(TableSchema::new(
            Arc::new(Schema::new(data_cols)),
            table_partition_cols,
        ))
    }

    fn build_fields(field_schemas: &Option<Vec<FieldSchema>>) -> Result<Vec<Arc<Field>>> {
        field_schemas
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|field_schema| {
                let name = field_schema.name.as_ref().ok_or_else(|| {
                    DataFusionError::Internal("FieldSchema's name not existed".to_string())
                })?;
                let data_type = field_schema.r#type.as_ref().ok_or_else(|| {
                    DataFusionError::Internal("FieldSchema's type not existed".to_string())
                })?;
                build_field(name, data_type)
            })
            .collect()
    }
}

fn build_field(name: &str, data_type: &str) -> Result<Arc<Field>> {
    Ok(Arc::new(Field::new(
        name,
        hive_type_to_arrow_type(data_type)?,
        true,
    )))
}
