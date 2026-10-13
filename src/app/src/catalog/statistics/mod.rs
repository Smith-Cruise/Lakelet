pub(crate) mod glue;
mod manager;

use async_trait::async_trait;
use datafusion::common::{ResolvedTableReference, Statistics};
use datafusion::datasource::table_schema::TableSchema;
use std::collections::HashMap;

pub use manager::StatisticsManager;

#[derive(Clone, Copy)]
pub(crate) struct TableStatisticsRequest<'a> {
    pub table_reference: &'a ResolvedTableReference,
    pub table_schema: &'a TableSchema,
    pub table_properties: &'a HashMap<String, String>,
}

impl<'a> TableStatisticsRequest<'a> {
    pub fn new(
        table_reference: &'a ResolvedTableReference,
        table_schema: &'a TableSchema,
        table_properties: &'a HashMap<String, String>,
    ) -> Self {
        Self {
            table_reference,
            table_schema,
            table_properties,
        }
    }
}

pub(crate) struct LoadedStatistics {
    pub statistics: Statistics,
    pub cacheable: bool,
}

impl LoadedStatistics {
    pub fn new(statistics: Statistics, cacheable: bool) -> Self {
        Self {
            statistics,
            cacheable,
        }
    }
}

#[async_trait]
pub(crate) trait TableStatisticsLoader: Send + Sync {
    async fn load(&self, request: TableStatisticsRequest<'_>) -> LoadedStatistics;
}
