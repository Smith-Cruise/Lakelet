use crate::table_format::TableFormat;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use std::collections::HashMap;

const HUDI_INPUT_FORMATS: &[&str] = &[
    "org.apache.hudi.hadoop.HoodieParquetInputFormat",
    "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat",
    "com.uber.hoodie.hadoop.HoodieInputFormat",
    "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat",
];

/// Detects the table format of a metastore (HMS / Glue) table from its
/// catalog metadata. Signals that are not provided are treated as absent.
#[derive(Debug, Default)]
pub struct TableFormatDetector<'a> {
    table_properties: Option<&'a HashMap<String, String>>,
    /// The metastore-level table type, e.g. `EXTERNAL_TABLE` or `VIRTUAL_VIEW`.
    table_type: Option<&'a str>,
    input_format: Option<&'a str>,
}

impl<'a> TableFormatDetector<'a> {
    pub fn with_table_properties(mut self, table_properties: &'a HashMap<String, String>) -> Self {
        self.table_properties = Some(table_properties);
        self
    }

    pub fn with_table_type(mut self, table_type: Option<&'a str>) -> Self {
        self.table_type = table_type;
        self
    }

    pub fn with_input_format(mut self, input_format: Option<&'a str>) -> Self {
        self.input_format = input_format;
        self
    }

    pub fn detect(&self) -> Result<TableFormat> {
        if self.is_iceberg() {
            return Ok(TableFormat::Iceberg);
        }
        if self.is_paimon() {
            return Ok(TableFormat::Paimon);
        }
        if self.is_delta() {
            return Ok(TableFormat::Delta);
        }

        // Hudi tables look like plain parquet Hive tables. Reading them as Hive
        // would return stale or duplicated rows, so reject them explicitly.
        if self.is_hudi() {
            return Err(DataFusionError::NotImplemented(
                "Hudi tables are not supported yet".to_string(),
            ));
        }
        if let Some(table_type) = self.view_type() {
            return Err(DataFusionError::NotImplemented(format!(
                "Hive views are not supported yet (table type: {table_type})"
            )));
        }
        // ACID tables keep uncompacted inserts and deletes in delta files that
        // a plain Hive scan would misread.
        if self.is_transactional() {
            return Err(DataFusionError::NotImplemented(
                "Hive transactional (ACID) tables are not supported yet".to_string(),
            ));
        }

        // Whether the Hive storage layout is actually readable is validated when
        // building the Hive storage info.
        Ok(TableFormat::Hive)
    }

    // Iceberg catalogs (HiveCatalog, GlueCatalog) set table_type=ICEBERG, the
    // same signal Trino uses.
    fn is_iceberg(&self) -> bool {
        self.property("table_type")
            .is_some_and(|table_type| table_type.eq_ignore_ascii_case("ICEBERG"))
    }

    fn is_paimon(&self) -> bool {
        self.property("table_type")
            .is_some_and(|table_type| table_type.eq_ignore_ascii_case("PAIMON"))
    }

    fn is_delta(&self) -> bool {
        // Glue crawlers register native Delta tables with table_type=DELTA.
        self.property("table_type")
            .is_some_and(|table_type| table_type.eq_ignore_ascii_case("DELTA"))
            || self
                .spark_provider()
                .is_some_and(|provider| provider.eq_ignore_ascii_case("DELTA"))
    }

    // Matches the Hudi input formats Trino recognizes.
    fn is_hudi(&self) -> bool {
        self.input_format
            .is_some_and(|input_format| HUDI_INPUT_FORMATS.contains(&input_format))
    }

    fn view_type(&self) -> Option<&'a str> {
        self.table_type.filter(|table_type| {
            table_type.eq_ignore_ascii_case("VIRTUAL_VIEW")
                || table_type.eq_ignore_ascii_case("MATERIALIZED_VIEW")
        })
    }

    fn is_transactional(&self) -> bool {
        ["transactional", "TRANSACTIONAL"].iter().any(|key| {
            self.property(key)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        })
    }

    fn spark_provider(&self) -> Option<&'a str> {
        self.property("spark.sql.sources.provider")
    }

    fn property(&self, key: &str) -> Option<&'a str> {
        self.table_properties?.get(key).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detect_from_properties(properties: &[(&str, &str)]) -> Result<TableFormat> {
        let table_properties = properties
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        TableFormatDetector::default()
            .with_table_properties(&table_properties)
            .detect()
    }

    #[test]
    fn detects_table_format_from_table_properties() {
        assert_eq!(
            TableFormat::Iceberg,
            detect_from_properties(&[("table_type", "ICEBERG"), ("metadata_location", "path")])
                .unwrap()
        );
        assert_eq!(
            TableFormat::Iceberg,
            detect_from_properties(&[("table_type", "iceberg")]).unwrap()
        );
        // metadata_location alone is not an Iceberg marker.
        assert_eq!(
            TableFormat::Hive,
            detect_from_properties(&[("metadata_location", "path")]).unwrap()
        );
        assert_eq!(
            TableFormat::Delta,
            detect_from_properties(&[("spark.sql.sources.provider", "DELTA")]).unwrap()
        );
        assert_eq!(
            TableFormat::Delta,
            detect_from_properties(&[("spark.sql.sources.provider", "delta")]).unwrap()
        );
        assert_eq!(
            TableFormat::Delta,
            detect_from_properties(&[("table_type", "DELTA")]).unwrap()
        );
        assert_eq!(
            TableFormat::Paimon,
            detect_from_properties(&[("table_type", "PAIMON")]).unwrap()
        );
        assert_eq!(
            TableFormat::Paimon,
            detect_from_properties(&[("table_type", "paimon")]).unwrap()
        );
        assert_eq!(TableFormat::Hive, detect_from_properties(&[]).unwrap());
        assert_eq!(
            TableFormat::Hive,
            TableFormatDetector::default().detect().unwrap()
        );
    }

    #[test]
    fn rejects_hudi_tables() {
        for input_format in HUDI_INPUT_FORMATS {
            let err = TableFormatDetector::default()
                .with_input_format(Some(input_format))
                .detect()
                .unwrap_err();
            assert!(err.to_string().contains("Hudi tables are not supported"));
        }
        // Only the input format identifies a Hudi table.
        assert_eq!(
            TableFormat::Hive,
            detect_from_properties(&[("hoodie.table.name", "t")]).unwrap()
        );
    }

    #[test]
    fn rejects_transactional_tables() {
        for properties in [
            &[("transactional", "true")][..],
            &[
                ("transactional", "TRUE"),
                ("transactional_properties", "insert_only"),
            ][..],
            &[("TRANSACTIONAL", "true")][..],
        ] {
            let err = detect_from_properties(properties).unwrap_err();
            assert!(
                err.to_string().contains("transactional (ACID) tables"),
                "{err}"
            );
        }
        assert_eq!(
            TableFormat::Hive,
            detect_from_properties(&[("transactional", "false")]).unwrap()
        );
    }

    #[test]
    fn rejects_views_and_falls_back_to_hive() {
        let err = TableFormatDetector::default()
            .with_table_type(Some("VIRTUAL_VIEW"))
            .detect()
            .unwrap_err();
        assert!(err.to_string().contains("views are not supported"));

        let table_format = TableFormatDetector::default()
            .with_table_type(Some("EXTERNAL_TABLE"))
            .with_input_format(Some(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            ))
            .detect()
            .unwrap();
        assert_eq!(TableFormat::Hive, table_format);
    }
}
