use crate::catalog::statistics::StatisticsManager;
use crate::catalog::{CatalogConfigs, CatalogManager};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use lakelet_common::runtime::RuntimeManager;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct LakeletConfig {
    #[serde(rename = "server")]
    pub server_config: Option<ServerConfig>,
    pub catalog: Option<CatalogConfigs>,
}

/// Used when `flight-sql-server-port` is not set in the config file.
pub const DEFAULT_FLIGHT_SQL_SERVER_PORT: u16 = 32010;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    #[serde(rename = "memory-limit", deserialize_with = "deserialize_memory_size")]
    pub memory_limit: Option<usize>,

    /// Port for the Arrow Flight SQL server started by `--flight-sql-server`.
    #[serde(rename = "flight-sql-server-port")]
    pub flight_sql_server_port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            memory_limit: None,
            flight_sql_server_port: DEFAULT_FLIGHT_SQL_SERVER_PORT,
        }
    }
}

fn deserialize_memory_size<'de, D>(deserializer: D) -> std::result::Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(s) => parse_memory_size(&s).map(Some).map_err(DeError::custom),
    }
}

fn parse_memory_size(size: &str) -> std::result::Result<usize, String> {
    let lower = size.trim().to_lowercase();
    let (num_part, suffix) = lower
        .find(|c: char| c.is_alphabetic())
        .map(|i| (&lower[..i], &lower[i..]))
        .unwrap_or((&lower, "b"));

    let num: usize = num_part
        .parse()
        .map_err(|_| format!("Invalid numeric value in memory-limit '{size}'"))?;

    let multiplier: usize = match suffix {
        "b" | "" => 1,
        "k" | "kb" => 1 << 10,
        "m" | "mb" => 1 << 20,
        "g" | "gb" => 1 << 30,
        "t" | "tb" => 1 << 40,
        _ => return Err(format!("Invalid memory-limit suffix in '{size}'")),
    };

    num.checked_mul(multiplier)
        .ok_or_else(|| format!("memory-limit '{size}' is too large"))
}

#[derive(Clone)]
pub struct LakeletContext {
    pub server_config: ServerConfig,
    pub catalog_manager: Arc<CatalogManager>,
    pub statistics_manager: Arc<StatisticsManager>,
    pub runtime_manager: Arc<RuntimeManager>,
    pub default_catalog: Option<String>,
    pub default_schema: Option<String>,
}

impl Default for LakeletContext {
    fn default() -> Self {
        Self {
            server_config: ServerConfig::default(),
            catalog_manager: Arc::new(CatalogManager::new()),
            statistics_manager: Arc::new(StatisticsManager::default()),
            runtime_manager: Arc::new(RuntimeManager::default()),
            default_catalog: None,
            default_schema: None,
        }
    }
}

impl LakeletContext {
    pub fn new(config_path: Option<&str>) -> Result<Self> {
        let Some(config_path) = config_path else {
            return Ok(Self::default());
        };

        let config = std::fs::read_to_string(config_path)?;
        let lakelet_config: LakeletConfig = toml::from_str(&config).map_err(|e| {
            DataFusionError::Configuration(format!("Failed to parse config: {}", e))
        })?;
        let mut catalog_manager = CatalogManager::new();
        catalog_manager.load_catalogs(&lakelet_config.catalog.unwrap_or_default())?;
        let server_config = lakelet_config.server_config.unwrap_or_default();
        // todo simplify code, try to reuse LakeletContext::default()
        Ok(Self {
            server_config,
            catalog_manager: Arc::new(catalog_manager),
            statistics_manager: Arc::new(StatisticsManager::default()),
            runtime_manager: Arc::new(RuntimeManager::default()),
            default_catalog: None,
            default_schema: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_server_config_from_server_table() {
        for memory_limit in ["4g", "4gb", "4GB"] {
            let config: LakeletConfig = toml::from_str(&format!(
                r#"
                [server]
                memory-limit = "{memory_limit}"
                "#
            ))
            .unwrap();

            assert_eq!(
                config.server_config.unwrap().memory_limit,
                Some(4 * 1024 * 1024 * 1024)
            );
        }
    }

    #[test]
    fn parse_flight_sql_server_config() {
        let config: LakeletConfig = toml::from_str(
            r#"
            [server]
            flight-sql-server-port = 12345
            "#,
        )
        .unwrap();

        let server_config = config.server_config.unwrap();
        assert_eq!(server_config.flight_sql_server_port, 12345);

        let config: LakeletConfig = toml::from_str(
            r#"
            [server]
            memory-limit = "1gb"
            "#,
        )
        .unwrap();

        let server_config = config.server_config.unwrap();
        assert_eq!(
            server_config.flight_sql_server_port,
            DEFAULT_FLIGHT_SQL_SERVER_PORT
        );
    }
}
