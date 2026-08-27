use aws_sdk_glue::types::Table as GlueTable;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::datasource::table_schema::TableSchema;
use hive_metastore::Table as HMSTable;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum HiveInputFormat {
    TextFile,
    Parquet,
    Orc,
}

#[derive(Debug, Clone)]
pub struct HiveStorageInfo {
    pub input_format: HiveInputFormat,
    pub table_schema: TableSchema,
    pub table_statistics: Statistics,
    pub serde_properties: HashMap<String, String>,
    pub table_properties: HashMap<String, String>,
}

impl HiveStorageInfo {
    pub fn try_new_from_hms_table(table: &HMSTable, table_schema: TableSchema, table_statistics: Statistics) -> Result<Self> {
        let sd = table.sd.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        let serde_properties = sd
            .serde_info
            .as_ref()
            .and_then(|s| s.parameters.as_ref())
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let table_properties: HashMap<String, String> = table
            .parameters
            .as_ref()
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Self::try_new(
            sd.input_format.as_deref(),
            table_schema,
            table_statistics,
            serde_properties,
            table_properties,
        )
    }

    pub fn try_new_from_glue_table(
        table: &GlueTable,
        table_schema: TableSchema,
        table_statistics: Statistics,
    ) -> Result<Self> {
        let sd = table.storage_descriptor.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        let serde_properties = sd
            .serde_info()
            .and_then(|s| s.parameters())
            .cloned()
            .unwrap_or_default();
        let table_properties: HashMap<String, String> =
            table.parameters.as_ref().cloned().unwrap_or_default();

        Self::try_new(
            sd.input_format(),
            table_schema,
            table_statistics,
            serde_properties,
            table_properties,
        )
    }

    pub fn try_get_input_format(input_format: &str) -> Result<HiveInputFormat> {
        if input_format.to_lowercase().contains("text") {
            Ok(HiveInputFormat::TextFile)
        } else if input_format.to_lowercase().contains("parquet") {
            Ok(HiveInputFormat::Parquet)
        } else if input_format.to_lowercase().contains("orc") {
            Ok(HiveInputFormat::Orc)
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "unsupported Hive input format: {}",
                input_format
            )))
        }
    }

    fn try_new(
        input_format: Option<&str>,
        table_schema: TableSchema,
        table_statistics: Statistics,
        serde_properties: HashMap<String, String>,
        table_properties: HashMap<String, String>,
    ) -> Result<Self> {
        let input_format = match input_format {
            Some(input_format) => Self::try_get_input_format(input_format)?,
            None => {
                return Err(DataFusionError::Internal(
                    "input format not existed".to_string(),
                ));
            }
        };

        // valid table_schema && table_statistics is vaild
        if table_statistics.column_statistics.len() != table_schema.table_schema().fields().len() {
            return Err(DataFusionError::Internal(format!(
                "statistics column count mismatch: statistics={}, schema={}",
                table_statistics.column_statistics.len(),
                table_schema.table_schema().fields().len()
            )));
        }
        Ok(Self {
            input_format,
            table_schema,
            table_statistics,
            serde_properties,
            table_properties,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_format::hive::HMSTableSchemaBuilder;
    use datafusion::common::stats::Precision;
    use hive_metastore::{FieldSchema, StorageDescriptor as HMSStorageDescriptor};

    const PARQUET_INPUT_FORMAT: &str =
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";

    #[test]
    fn hms_table_initializes_unknown_statistics() {
        let field = FieldSchema {
            name: Some("id".into()),
            r#type: Some("bigint".into()),
            ..Default::default()
        };
        let storage_descriptor = HMSStorageDescriptor {
            cols: Some(vec![field]),
            input_format: Some(PARQUET_INPUT_FORMAT.into()),
            ..Default::default()
        };
        let table = HMSTable {
            sd: Some(storage_descriptor),
            parameters: Some([("numRows".into(), "42".into())].into_iter().collect()),
            ..Default::default()
        };

        let table_schema = HMSTableSchemaBuilder::new(&table).build().unwrap();
        let table_statistics = Statistics::new_unknown(table_schema.table_schema());
        let info = HiveStorageInfo::try_new_from_hms_table(&table, table_schema, table_statistics).unwrap();

        assert_eq!(info.table_statistics.num_rows, Precision::Absent);
        assert_eq!(info.table_statistics.total_byte_size, Precision::Absent);
        assert!(
            info.table_statistics
                .column_statistics
                .iter()
                .all(|statistics| *statistics
                    == datafusion::common::stats::ColumnStatistics::new_unknown())
        );
    }
}
