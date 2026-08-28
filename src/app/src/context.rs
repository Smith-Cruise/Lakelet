use crate::catalog::statistics::StatisticsManager;
use crate::catalog::{CatalogConfigs, CatalogManager};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use lakelet_common::runtime::RuntimeManager;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::Arc;
use sysinfo::System;

const DEFAULT_MEMORY_LIMIT_PERCENT: u64 = 80;
const MEMORY_LIMIT_CONFIG_HINT: &str =
    "set 'memory-limit' explicitly under [server] in the configuration file";

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

impl ServerConfig {
    pub fn resolve_memory_limit(&self) -> Result<usize> {
        if let Some(memory_limit) = self.memory_limit {
            return Ok(memory_limit);
        }

        let system = System::new_all();
        let physical_total_memory = Some(system.total_memory());
        let cgroup_memory_limit = current_process_cgroup_memory_limit(&system)?;
        calculate_memory_limit(None, physical_total_memory, cgroup_memory_limit)
    }
}

#[cfg(target_os = "linux")]
fn current_process_cgroup_memory_limit(system: &System) -> Result<Option<u64>> {
    let pid = sysinfo::get_current_pid().map_err(|error| {
        DataFusionError::Configuration(format!(
            "Failed to identify the current process while determining the default memory limit: {error}; {MEMORY_LIMIT_CONFIG_HINT}"
        ))
    })?;
    let process = system.process(pid).ok_or_else(|| {
        DataFusionError::Configuration(format!(
            "Failed to inspect the current process while determining the default memory limit; {MEMORY_LIMIT_CONFIG_HINT}"
        ))
    })?;
    Ok(process.cgroup_limits().map(|limits| limits.total_memory))
}

#[cfg(not(target_os = "linux"))]
fn current_process_cgroup_memory_limit(_system: &System) -> Result<Option<u64>> {
    Ok(None)
}

fn calculate_memory_limit(
    configured_memory_limit: Option<usize>,
    physical_total_memory: Option<u64>,
    cgroup_memory_limit: Option<u64>,
) -> Result<usize> {
    if let Some(memory_limit) = configured_memory_limit {
        return Ok(memory_limit);
    }

    let physical_total_memory = physical_total_memory.filter(|memory| *memory > 0).ok_or_else(|| {
        DataFusionError::Configuration(format!(
            "Physical memory is unavailable while determining the default memory limit; {MEMORY_LIMIT_CONFIG_HINT}"
        ))
    })?;
    let effective_total_memory = match cgroup_memory_limit {
        Some(0) => {
            return Err(DataFusionError::Configuration(format!(
                "The cgroup memory limit is invalid while determining the default memory limit; {MEMORY_LIMIT_CONFIG_HINT}"
            )));
        }
        Some(cgroup_memory_limit) => physical_total_memory.min(cgroup_memory_limit),
        None => physical_total_memory,
    };
    let memory_limit =
        u128::from(effective_total_memory) * u128::from(DEFAULT_MEMORY_LIMIT_PERCENT) / 100;
    if memory_limit == 0 {
        return Err(DataFusionError::Configuration(format!(
            "The detected memory capacity is too small to calculate the default memory limit; {MEMORY_LIMIT_CONFIG_HINT}"
        )));
    }

    usize::try_from(memory_limit).map_err(|_| {
        DataFusionError::Configuration(format!(
            "The calculated default memory limit cannot be represented on this platform; {MEMORY_LIMIT_CONFIG_HINT}"
        ))
    })
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
    fn calculate_default_memory_limit_from_effective_total_memory() {
        const GIB: u64 = 1024 * 1024 * 1024;

        for (physical, cgroup, expected_total) in [
            (16 * GIB, None, 16 * GIB),
            (64 * GIB, Some(8 * GIB), 8 * GIB),
            (8 * GIB, Some(64 * GIB), 8 * GIB),
        ] {
            assert_eq!(
                calculate_memory_limit(None, Some(physical), cgroup).unwrap(),
                usize::try_from(expected_total * 80 / 100).unwrap()
            );
        }
    }

    #[test]
    fn configured_memory_limit_takes_priority() {
        assert_eq!(
            calculate_memory_limit(Some(123), None, Some(0)).unwrap(),
            123
        );
    }

    #[test]
    fn invalid_memory_detection_requires_explicit_configuration() {
        for result in [
            calculate_memory_limit(None, None, None),
            calculate_memory_limit(None, Some(0), None),
            calculate_memory_limit(None, Some(1), None),
            calculate_memory_limit(None, Some(1024), Some(0)),
        ] {
            let error = result.unwrap_err().to_string();
            assert!(error.contains("set 'memory-limit' explicitly"));
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
