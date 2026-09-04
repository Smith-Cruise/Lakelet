use bytes::Bytes;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::{ParquetFileMetrics, ParquetFileReaderFactory};
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::object_store::{
    OBJECT_STORE_COALESCE_DEFAULT, ObjectStore, ObjectStoreExt, coalesce_ranges,
};
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::ops::Range;
use std::sync::Arc;
use tokio::runtime::Handle;

/// Tunables for the readers handed out by [`ExtendedParquetFileReaderFactory`].
///
/// Kept as a struct rather than positional arguments so that adding a reader
/// tunable later does not churn every call site.
#[derive(Debug, Clone, Default)]
pub struct ExtendedParquetReaderOptions {
    /// Process-wide parquet metadata cache, normally
    /// `state.runtime_env().cache_manager.get_file_metadata_cache()`.
    /// `None` falls back to reading the footer on every scan.
    pub metadata_cache: Option<Arc<dyn FileMetadataCache>>,
}

#[derive(Debug)]
pub struct ExtendedParquetFileReaderFactory {
    io_handle: Handle,
    store: Arc<dyn ObjectStore>,
    options: ExtendedParquetReaderOptions,
}

impl ExtendedParquetFileReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        io_handle: Handle,
        options: ExtendedParquetReaderOptions,
    ) -> Self {
        Self {
            store,
            io_handle,
            options,
        }
    }
}

impl ParquetFileReaderFactory for ExtendedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            partitioned_file.object_meta.location.as_ref(),
            metrics,
        );

        Ok(Box::new(ExtendedParquetFileReader {
            file_metrics,
            store: Arc::clone(&self.store),
            partitioned_file,
            io_handle: self.io_handle.clone(),
            options: self.options.clone(),
            metadata_size_hint,
        }))
    }
}

/// Reads a parquet file from object storage, keeping every byte of IO on the
/// dedicated IO runtime and serving the file metadata from a
/// [`FileMetadataCache`] when one is configured.
struct ExtendedParquetFileReader {
    file_metrics: ParquetFileMetrics,
    store: Arc<dyn ObjectStore>,
    partitioned_file: PartitionedFile,
    io_handle: Handle,
    options: ExtendedParquetReaderOptions,
    metadata_size_hint: Option<usize>,
}

impl ExtendedParquetFileReader {
    /// Single funnel for all data IO: both `AsyncFileReader` byte entry points
    /// route through here.
    ///
    /// Ranges close to each other are merged into a single object store request,
    /// which matters because the store backing this reader is an opendal store
    /// that overrides `ObjectStore::get_ranges` with one read per range and so
    /// loses the coalescing the `object_store` default implementation provides.
    fn fetch_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, ParquetResult<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total as usize);

        let store = Arc::clone(&self.store);
        let path = self.partitioned_file.object_meta.location.clone();
        self.spawn_on_io(async move {
            coalesce_ranges(
                &ranges,
                |range| store.get_range(&path, range),
                OBJECT_STORE_COALESCE_DEFAULT,
            )
            .await
            .map_err(ParquetError::from)
        })
    }

    /// Submits an IO task to the dedicated IO runtime. Shared by the data fetch
    /// above and the metadata fetch below.
    fn spawn_on_io<F, T>(&self, future: F) -> BoxFuture<'static, ParquetResult<T>>
    where
        F: Future<Output = ParquetResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let join_handle = self.io_handle.spawn(future);
        async move {
            match join_handle.await {
                Ok(res) => res,
                // Surface a panicking IO task as a panic instead of hiding it
                // behind an error, the same way `ParquetObjectReader` does.
                Err(e) => match e.try_into_panic() {
                    Ok(p) => std::panic::resume_unwind(p),
                    Err(e) => Err(ParquetError::External(Box::new(e))),
                },
            }
        }
        .boxed()
    }
}

impl AsyncFileReader for ExtendedParquetFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let fetch = self.fetch_ranges(vec![range]);
        async move {
            let mut bytes = fetch.await?;
            // `fetch_ranges` returns one buffer per requested range.
            bytes.pop().ok_or_else(|| {
                ParquetError::General("Parquet range request returned no data".to_string())
            })
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>>
    where
        Self: Send,
    {
        self.fetch_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        // `PageIndexPolicy` is `Copy`; read it out here so the spawned task does
        // not borrow `options`.
        let page_index_policy: Option<PageIndexPolicy> =
            options.map(|options| options.column_index_policy());
        let store = Arc::clone(&self.store);
        let object_meta = self.partitioned_file.object_meta.clone();
        let metadata_cache = self.options.metadata_cache.clone();
        let metadata_size_hint = self.metadata_size_hint;

        self.spawn_on_io(async move {
            DFParquetMetadata::new(&store, &object_meta)
                .with_file_metadata_cache(metadata_cache)
                .with_metadata_size_hint(metadata_size_hint)
                .with_page_index_policy(page_index_policy)
                .fetch_metadata()
                .await
                .map_err(|e| {
                    ParquetError::General(format!(
                        "Failed to fetch metadata for file {}: {e}",
                        object_meta.location,
                    ))
                })
        })
    }
}

impl Drop for ExtendedParquetFileReader {
    fn drop(&mut self) {
        self.file_metrics
            .scan_efficiency_ratio
            .add_part(self.file_metrics.bytes_scanned.value());
        // Multiple readers may run, so set_total avoids adding the total multiple times.
        self.file_metrics
            .scan_efficiency_ratio
            .set_total(self.partitioned_file.object_meta.size as usize);
    }
}
