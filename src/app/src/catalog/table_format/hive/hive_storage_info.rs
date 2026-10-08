use crate::table_format::hive::hive_textfile_serde::TextFileSerdeProperties;
use aws_sdk_glue::types::Table as GlueTable;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::datasource::table_schema::TableSchema;
use hive_metastore::Table as HMSTable;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum HiveInputFormat {
    TextFile(TextFileSerdeProperties),
    Parquet,
    Orc,
    Avro,
}

#[derive(Debug, Clone)]
pub struct HiveStorageInfo {
    pub input_format: HiveInputFormat,
    pub table_schema: TableSchema,
    pub table_statistics: Statistics,
}

const TEXT_INPUT_FORMAT: &str = "org.apache.hadoop.mapred.TextInputFormat";
const PARQUET_INPUT_FORMATS: &[&str] = &[
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "parquet.hive.DeprecatedParquetInputFormat",
    "parquet.hive.MapredParquetInputFormat",
];
const ORC_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
const LAZY_SIMPLE_SERDE: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
const AVRO_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";

impl HiveStorageInfo {
    pub fn try_new_from_hms_table(
        table_schema: TableSchema,
        table_statistics: Statistics,
        table: &HMSTable,
    ) -> Result<Self> {
        let sd = table.sd.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        let serde_info = sd.serde_info.as_ref();
        let serde_properties = merge_serde_properties(
            table.parameters.iter().flatten(),
            serde_info
                .and_then(|serde_info| serde_info.parameters.as_ref())
                .into_iter()
                .flatten(),
        );

        Self::try_new(
            sd.input_format.as_deref(),
            serde_info.and_then(|serde_info| serde_info.serialization_lib.as_deref()),
            &serde_properties,
            table_schema,
            table_statistics,
        )
    }

    pub fn try_new_from_glue_table(
        table_schema: TableSchema,
        table_statistics: Statistics,
        table: &GlueTable,
    ) -> Result<Self> {
        let sd = table.storage_descriptor.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        let serde_info = sd.serde_info();
        let serde_properties = merge_serde_properties(
            table.parameters().into_iter().flatten(),
            serde_info
                .and_then(|serde_info| serde_info.parameters())
                .into_iter()
                .flatten(),
        );

        Self::try_new(
            sd.input_format(),
            serde_info.and_then(|serde_info| serde_info.serialization_library()),
            &serde_properties,
            table_schema,
            table_statistics,
        )
    }

    /// Resolves the storage layout from the input format and SerDe class names.
    /// Only layouts we can read correctly are accepted; everything else fails
    /// loudly instead of being read as a different format. `serde_properties`
    /// are the merged table and SerDe parameters.
    pub fn try_get_input_format(
        input_format: &str,
        serde_lib: Option<&str>,
        serde_properties: &HashMap<String, String>,
    ) -> Result<HiveInputFormat> {
        let unsupported = || {
            DataFusionError::NotImplemented(format!(
                "unsupported Hive storage format: input format {input_format}, SerDe {}",
                serde_lib.unwrap_or("<none>")
            ))
        };

        if input_format == TEXT_INPUT_FORMAT {
            return match serde_lib {
                None | Some(LAZY_SIMPLE_SERDE) => Ok(HiveInputFormat::TextFile(
                    TextFileSerdeProperties::try_new(serde_properties)?,
                )),
                Some(_) => Err(unsupported()),
            };
        }
        if PARQUET_INPUT_FORMATS.contains(&input_format) {
            return Ok(HiveInputFormat::Parquet);
        }
        if input_format == ORC_INPUT_FORMAT {
            return Ok(HiveInputFormat::Orc);
        }
        if input_format == AVRO_INPUT_FORMAT {
            return Ok(HiveInputFormat::Avro);
        }
        Err(unsupported())
    }

    fn try_new(
        input_format: Option<&str>,
        serde_lib: Option<&str>,
        serde_properties: &HashMap<String, String>,
        table_schema: TableSchema,
        table_statistics: Statistics,
    ) -> Result<Self> {
        let input_format = match input_format {
            Some(input_format) => {
                Self::try_get_input_format(input_format, serde_lib, serde_properties)?
            }
            None => {
                return Err(DataFusionError::Internal(
                    "input format not existed".to_string(),
                ));
            }
        };

        // validate that statistics cover every column of the table schema
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
        })
    }
}

/// Hive passes table parameters and SerDe parameters to the SerDe as one
/// property set, with SerDe parameters taking precedence. Some engines (e.g.
/// Trino) store delimiters as table parameters, so both must be consulted.
fn merge_serde_properties<'a, K, V>(
    table_parameters: impl IntoIterator<Item = (&'a K, &'a V)>,
    serde_parameters: impl IntoIterator<Item = (&'a K, &'a V)>,
) -> HashMap<String, String>
where
    K: ToString + 'a + ?Sized,
    V: ToString + 'a + ?Sized,
{
    table_parameters
        .into_iter()
        .chain(serde_parameters)
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_format::hive::HMSTableSchemaBuilder;
    use datafusion::common::stats::Precision;
    use hive_metastore::{FieldSchema, SerDeInfo, StorageDescriptor as HMSStorageDescriptor};

    const PARQUET_INPUT_FORMAT: &str =
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";

    #[test]
    fn resolve_input_format_whitelist() {
        let no_properties = HashMap::new();
        assert!(matches!(
            HiveStorageInfo::try_get_input_format(TEXT_INPUT_FORMAT, None, &no_properties).unwrap(),
            HiveInputFormat::TextFile(_)
        ));
        assert!(matches!(
            HiveStorageInfo::try_get_input_format(
                TEXT_INPUT_FORMAT,
                Some(LAZY_SIMPLE_SERDE),
                &no_properties
            )
            .unwrap(),
            HiveInputFormat::TextFile(_)
        ));
        assert!(matches!(
            HiveStorageInfo::try_get_input_format(PARQUET_INPUT_FORMAT, None, &no_properties)
                .unwrap(),
            HiveInputFormat::Parquet
        ));
        assert!(matches!(
            HiveStorageInfo::try_get_input_format(ORC_INPUT_FORMAT, None, &no_properties).unwrap(),
            HiveInputFormat::Orc
        ));
        assert!(matches!(
            HiveStorageInfo::try_get_input_format(AVRO_INPUT_FORMAT, None, &no_properties).unwrap(),
            HiveInputFormat::Avro
        ));
    }

    #[test]
    fn serde_parameters_override_table_parameters() {
        let table_parameters = HashMap::from([
            ("field.delim".to_string(), "|".to_string()),
            ("numRows".to_string(), "1".to_string()),
        ]);
        let serde_parameters = HashMap::from([("field.delim".to_string(), ",".to_string())]);

        let merged = merge_serde_properties(&table_parameters, &serde_parameters);
        assert_eq!(merged.get("field.delim").map(String::as_str), Some(","));
        assert_eq!(merged.get("numRows").map(String::as_str), Some("1"));

        let merged = merge_serde_properties(&table_parameters, &HashMap::new());
        assert_eq!(merged.get("field.delim").map(String::as_str), Some("|"));
    }

    #[test]
    fn textfile_serde_properties_include_table_parameters() {
        let table = HMSTable {
            sd: Some(HMSStorageDescriptor {
                cols: Some(vec![FieldSchema {
                    name: Some("id".into()),
                    r#type: Some("bigint".into()),
                    ..Default::default()
                }]),
                input_format: Some(TEXT_INPUT_FORMAT.into()),
                serde_info: Some(SerDeInfo {
                    serialization_lib: Some(LAZY_SIMPLE_SERDE.into()),
                    parameters: Some([("serialization.format".into(), "|".into())].into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            // Trino may store the delimiter as a table parameter.
            parameters: Some([("field.delim".into(), "|".into())].into_iter().collect()),
            ..Default::default()
        };

        let table_schema = HMSTableSchemaBuilder::new(&table).build().unwrap();
        let table_statistics = Statistics::new_unknown(table_schema.table_schema());
        let info = HiveStorageInfo::try_new_from_hms_table(table_schema, table_statistics, &table)
            .unwrap();
        let HiveInputFormat::TextFile(serde) = info.input_format else {
            panic!("expected TextFile, got {:?}", info.input_format);
        };
        assert_eq!(serde.field_delimiter, b'|');
    }

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
        let info = HiveStorageInfo::try_new_from_hms_table(table_schema, table_statistics, &table)
            .unwrap();

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
