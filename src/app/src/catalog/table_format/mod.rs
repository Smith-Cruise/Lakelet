pub mod delta;
pub mod hive;
pub mod iceberg;
mod metadata_table;
pub mod paimon;
pub mod table_format_detector;
pub mod table_provider_factory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    Iceberg,
    Delta,
    Hive,
    Paimon,
}

impl std::str::FromStr for TableFormat {
    type Err = datafusion::common::DataFusionError;

    fn from_str(format: &str) -> Result<Self, Self::Err> {
        match format.to_ascii_uppercase().as_str() {
            "ICEBERG" => Ok(Self::Iceberg),
            "DELTA" => Ok(Self::Delta),
            "HIVE" => Ok(Self::Hive),
            "PAIMON" => Ok(Self::Paimon),
            _ => datafusion::common::plan_err!("Unknown table format: {format}"),
        }
    }
}
