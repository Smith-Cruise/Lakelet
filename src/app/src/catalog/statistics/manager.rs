use super::{TableStatisticsLoader, TableStatisticsRequest};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, Statistics, TableReference};
use moka::sync::Cache;
use std::time::Duration;

const DEFAULT_STATISTICS_CACHE_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
struct CachedStatistics {
    table_schema: SchemaRef,
    statistics: Statistics,
}

pub struct StatisticsManager {
    cache: Cache<TableReference, CachedStatistics>,
}

impl Default for StatisticsManager {
    fn default() -> Self {
        Self::new(DEFAULT_STATISTICS_CACHE_TTL)
    }
}

impl StatisticsManager {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            cache: Cache::builder().time_to_live(ttl).build(),
        }
    }

    pub(crate) async fn load(
        &self,
        request: TableStatisticsRequest<'_>,
        loader: &dyn TableStatisticsLoader,
    ) -> Result<Statistics> {
        if !matches!(request.table_reference, TableReference::Full { .. }) {
            return Err(DataFusionError::Internal(
                "table statistics require a full table reference".to_string(),
            ));
        }

        if let Some(cached) = self.cache.get(request.table_reference) {
            if cached.table_schema.as_ref() == request.table_schema.table_schema().as_ref() {
                return Ok(cached.statistics);
            }
            self.cache.invalidate(request.table_reference);
        }

        let cache_key = request.table_reference.clone();
        let loaded = loader.load(request).await;
        if loaded.cacheable {
            self.cache.insert(
                cache_key,
                CachedStatistics {
                    table_schema: request.table_schema.table_schema().clone(),
                    statistics: loaded.statistics.clone(),
                },
            );
        }
        Ok(loaded.statistics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::statistics::LoadedStatistics;
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::datasource::table_schema::TableSchema;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    struct FakeLoader {
        calls: AtomicUsize,
        cacheable: bool,
        num_rows: usize,
        yield_before_return: bool,
    }

    impl FakeLoader {
        fn new(cacheable: bool, num_rows: usize) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                cacheable,
                num_rows,
                yield_before_return: false,
            }
        }
    }

    #[async_trait]
    impl TableStatisticsLoader for FakeLoader {
        async fn load(&self, request: TableStatisticsRequest<'_>) -> LoadedStatistics {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.yield_before_return {
                tokio::task::yield_now().await;
            }
            let mut statistics = Statistics::new_unknown(request.table_schema.table_schema());
            statistics.num_rows = Precision::Inexact(self.num_rows);
            LoadedStatistics::new(statistics, self.cacheable)
        }
    }

    fn make_table_schema(fields: Vec<Field>) -> TableSchema {
        TableSchema::new(Arc::new(Schema::new(fields)), vec![])
    }

    #[tokio::test]
    async fn reuses_cached_statistics_only_for_matching_schema() {
        let manager = StatisticsManager::new(Duration::from_secs(60));
        let loader = FakeLoader::new(true, 10);
        let table_reference = TableReference::full("catalog", "schema", "table");
        let table_schema = make_table_schema(vec![Field::new("id", DataType::Int64, true)]);
        let properties = HashMap::new();
        let request = TableStatisticsRequest::new(&table_reference, &table_schema, &properties);

        let first = manager.load(request, &loader).await.unwrap();
        let second = manager.load(request, &loader).await.unwrap();

        assert_eq!(first, second);
        assert_eq!(loader.calls.load(Ordering::SeqCst), 1);

        let renamed_schema =
            make_table_schema(vec![Field::new("renamed_id", DataType::Int64, true)]);
        let renamed = manager
            .load(
                TableStatisticsRequest::new(&table_reference, &renamed_schema, &properties),
                &loader,
            )
            .await
            .unwrap();
        assert_eq!(renamed.column_statistics.len(), 1);
        assert_eq!(loader.calls.load(Ordering::SeqCst), 2);

        let expanded_schema = make_table_schema(vec![
            Field::new("renamed_id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let expanded = manager
            .load(
                TableStatisticsRequest::new(&table_reference, &expanded_schema, &properties),
                &loader,
            )
            .await
            .unwrap();
        assert_eq!(expanded.column_statistics.len(), 2);
        assert_eq!(loader.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn isolates_statistics_across_catalogs() {
        let manager = StatisticsManager::new(Duration::from_secs(60));
        let first_loader = FakeLoader::new(true, 10);
        let second_loader = FakeLoader::new(true, 20);
        let table_schema = make_table_schema(vec![Field::new("id", DataType::Int64, true)]);
        let properties = HashMap::new();
        let first_table = TableReference::full("first", "schema", "table");
        let second_table = TableReference::full("second", "schema", "table");

        let first = manager
            .load(
                TableStatisticsRequest::new(&first_table, &table_schema, &properties),
                &first_loader,
            )
            .await
            .unwrap();
        let second = manager
            .load(
                TableStatisticsRequest::new(&second_table, &table_schema, &properties),
                &second_loader,
            )
            .await
            .unwrap();

        assert_eq!(first.num_rows, Precision::Inexact(10));
        assert_eq!(second.num_rows, Precision::Inexact(20));
        assert_eq!(first_loader.calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_loader.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reloads_expired_statistics() {
        let manager = StatisticsManager::new(Duration::from_millis(1));
        let loader = FakeLoader::new(true, 10);
        let table_reference = TableReference::full("catalog", "schema", "table");
        let table_schema = make_table_schema(vec![Field::new("id", DataType::Int64, true)]);
        let properties = HashMap::new();
        let request = TableStatisticsRequest::new(&table_reference, &table_schema, &properties);

        manager.load(request, &loader).await.unwrap();
        thread::sleep(Duration::from_millis(10));
        manager.load(request, &loader).await.unwrap();

        assert_eq!(loader.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn does_not_cache_uncacheable_statistics() {
        let manager = StatisticsManager::new(Duration::from_secs(60));
        let loader = FakeLoader::new(false, 10);
        let table_reference = TableReference::full("catalog", "schema", "table");
        let table_schema = make_table_schema(vec![Field::new("id", DataType::Int64, true)]);
        let properties = HashMap::new();
        let request = TableStatisticsRequest::new(&table_reference, &table_schema, &properties);

        manager.load(request, &loader).await.unwrap();
        manager.load(request, &loader).await.unwrap();

        assert_eq!(loader.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn rejects_non_full_table_references() {
        let manager = StatisticsManager::default();
        let loader = FakeLoader::new(true, 10);
        let table_reference = TableReference::bare("table");
        let table_schema = make_table_schema(vec![Field::new("id", DataType::Int64, true)]);
        let properties = HashMap::new();
        let request = TableStatisticsRequest {
            table_reference: &table_reference,
            table_schema: &table_schema,
            table_properties: &properties,
        };

        let error = manager.load(request, &loader).await.unwrap_err();

        assert!(error.to_string().contains("full table reference"));
    }
}
