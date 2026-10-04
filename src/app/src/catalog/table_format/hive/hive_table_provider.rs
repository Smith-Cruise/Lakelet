use crate::data_file_format::case_insensitive_adapter::CaseInsensitivePhysicalExprAdapterFactory;
use crate::data_file_format::parquet::{
    ExtendedParquetFileReaderFactory, ExtendedParquetReaderOptions,
};
use crate::table_format::hive::HiveStorageInfo;
use crate::table_format::hive::hive_file_utils::{list_files, list_files_by_directories};
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveInputFormat;
use crate::table_format::hive::hive_textfile_serde::TextFileSerdeProperties;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::stats::Precision;
use datafusion::common::{Column, ColumnStatistics, Result, Statistics, ToDFSchema};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    CsvSource, FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, lit, try_cast, when};
use datafusion::object_store::path::Path;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use lakelet_storage::storage::{
    Storage, parse_location_schema_authority, try_register_storage_info_session,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug)]
pub struct HiveTableProvider {
    table_location: String,
    hive_storage_info: HiveStorageInfo,
    partitions: Vec<HivePartition>,
    storage: Storage,
    io_handle: Handle,
    table_definition: String,
}

impl HiveTableProvider {
    pub fn new(
        table_location: String,
        hive_storage_info: HiveStorageInfo,
        partitions: Vec<HivePartition>,
        storage: Storage,
        io_handle: Handle,
        table_definition: String,
    ) -> Self {
        Self {
            table_location,
            hive_storage_info,
            partitions,
            storage,
            io_handle,
            table_definition,
        }
    }
}

#[async_trait]
impl TableProvider for HiveTableProvider {
    fn schema(&self) -> SchemaRef {
        self.hive_storage_info.table_schema.table_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(&self.table_definition)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        try_register_storage_info_session(&self.storage, &self.table_location, state)?;

        let (path_schema, path_bucket) = parse_location_schema_authority(&self.table_location)?;
        let store_url = ObjectStoreUrl::parse(format!("{}://{}", path_schema, path_bucket))?;

        let object_store = state.runtime_env().object_store(&store_url)?;
        let meta_fetch_concurrency = state.config_options().execution.meta_fetch_concurrency;

        let mut partition_pruned = false;
        let is_partitioned = !self
            .hive_storage_info
            .table_schema
            .table_partition_cols()
            .is_empty();
        let scan_file_list: Vec<PartitionedFile> = if !is_partitioned {
            let file_object_metas =
                list_files_by_directories(state, &object_store, vec![self.table_location.clone()])
                    .await?;
            file_object_metas
                .into_iter()
                .map(PartitionedFile::from)
                .collect()
        } else {
            let selected_partition_indices = prune_partitions(
                &self.partitions,
                self.hive_storage_info.table_schema.table_partition_cols(),
                filters,
                state,
            )?;
            partition_pruned = selected_partition_indices.len() < self.partitions.len();

            let partition_scan_tasks = selected_partition_indices
                .into_iter()
                .map(|partition_idx| {
                    let partition = &self.partitions[partition_idx];
                    let location = partition.location.clone();
                    let partition_values = build_partition_values(
                        self.hive_storage_info.table_schema.table_partition_cols(),
                        partition,
                    )?;
                    let object_store = Arc::clone(&object_store);

                    Ok(async move {
                        let file_object_metas = list_files(state, &object_store, &location).await?;
                        let partitioned_files: Vec<_> = file_object_metas
                            .into_iter()
                            .map(|file_object_meta| {
                                let mut partitioned_file = PartitionedFile::from(file_object_meta);
                                partitioned_file.partition_values = partition_values.clone();
                                partitioned_file
                            })
                            .collect();

                        Ok(partitioned_files)
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            futures::stream::iter(partition_scan_tasks)
                .buffer_unordered(meta_fetch_concurrency)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect()
        };

        let statistics = try_fill_table_statistics_by_file_list(
            self.hive_storage_info.table_statistics.clone(),
            self.hive_storage_info.table_schema.table_schema().clone(),
            &scan_file_list,
            partition_pruned,
        );
        let file_group = FileGroup::new(scan_file_list);

        let exec = match &self.hive_storage_info.input_format {
            HiveInputFormat::TextFile(serde_properties) => build_csv_exec(
                store_url,
                &self.hive_storage_info.table_schema,
                file_group,
                serde_properties,
                state,
                statistics,
                projection,
                limit,
            ),
            HiveInputFormat::Parquet => build_parquet_exec(
                self.io_handle.clone(),
                store_url,
                self.hive_storage_info.table_schema.clone(),
                file_group,
                state,
                statistics,
                projection,
                limit,
            ),
            HiveInputFormat::Orc => {
                return Err(DataFusionError::NotImplemented(
                    "orc not implemented".to_string(),
                ));
            }
        }?;

        Ok(exec)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

// The CSV scan builder mirrors DataFusion scan inputs, so keeping these
// arguments explicit is clearer than wrapping them only to satisfy clippy.
#[allow(clippy::too_many_arguments)]
fn build_csv_exec(
    store_url: ObjectStoreUrl,
    table_schema: &TableSchema,
    file_group: FileGroup,
    serde_properties: &TextFileSerdeProperties,
    state: &dyn Session,
    statistics: Statistics,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let options = build_textfile_csv_options(serde_properties, &file_group)?;
    let file_compression = FileCompressionType::from(options.compression);

    // Every data column is read as text and converted afterwards, so that the
    // Hive null format and Hive's lenient parsing (bad values become NULL) can
    // be applied. The CSV reader supports neither.
    let file_schema = table_schema.file_schema();
    let text_table_schema = TableSchema::new(
        Arc::new(Schema::new(
            file_schema
                .fields()
                .iter()
                .map(|field| Field::new(field.name(), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        )),
        table_schema.table_partition_cols().clone(),
    );
    let text_statistics = Statistics {
        column_statistics: statistics
            .column_statistics
            .into_iter()
            .enumerate()
            .map(|(index, column_statistics)| {
                if index < file_schema.fields().len() {
                    // min/max are typed with the Hive column type, not text.
                    ColumnStatistics {
                        null_count: column_statistics.null_count,
                        distinct_count: column_statistics.distinct_count,
                        ..ColumnStatistics::new_unknown()
                    }
                } else {
                    column_statistics
                }
            })
            .collect(),
        ..statistics
    };

    let source = Arc::new(CsvSource::new(text_table_schema).with_csv_options(options));
    let mut builder = FileScanConfigBuilder::new(store_url, source).with_file_group(file_group);
    builder = builder.with_statistics(text_statistics);
    builder = builder.with_file_compression_type(file_compression);
    if let Some(proj) = projection {
        builder = builder.with_projection_indices(Some(proj.clone()))?;
    }
    if let Some(lim) = limit {
        builder = builder.with_limit(Some(lim));
    }
    let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(builder.build());

    let null_format = serde_properties.null_format.as_str();
    let scan_schema = scan.schema();
    let scan_df_schema = scan_schema.clone().to_dfschema()?;
    let table_schema = table_schema.table_schema();
    let projected_indices: Vec<usize> = match projection {
        Some(projection) => projection.clone(),
        None => (0..table_schema.fields().len()).collect(),
    };
    if projected_indices
        .iter()
        .all(|index| *index >= file_schema.fields().len())
    {
        return Ok(scan);
    }
    let exprs = projected_indices
        .iter()
        .zip(scan_schema.fields())
        .map(|(table_index, scan_field)| {
            let column = datafusion::logical_expr::col(Column::from_name(scan_field.name()));
            let expr = if *table_index < file_schema.fields().len() {
                let target_type = table_schema.field(*table_index).data_type();
                if target_type.is_nested() {
                    // Complex values are not decoded from text yet, so they read as NULL.
                    return Ok((
                        state.create_physical_expr(
                            lit(ScalarValue::try_from(target_type)?),
                            &scan_df_schema,
                        )?,
                        scan_field.name().to_string(),
                    ));
                }
                let value = if target_type == &DataType::Utf8 {
                    column.clone()
                } else {
                    try_cast(column.clone(), target_type.clone())
                };
                when(
                    column.eq(lit(null_format)),
                    lit(ScalarValue::try_from(target_type)?),
                )
                .otherwise(value)?
            } else {
                column
            };
            Ok((
                state.create_physical_expr(expr, &scan_df_schema)?,
                scan_field.name().to_string(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(ProjectionExec::try_new(exprs, scan)?))
}

/// Maps LazySimpleSerDe properties onto CSV reader options, rejecting the ones
/// the CSV reader cannot honor.
fn build_textfile_csv_options(
    serde_properties: &TextFileSerdeProperties,
    file_group: &FileGroup,
) -> Result<CsvOptions> {
    if serde_properties.line_delimiter != b'\n' {
        return Err(DataFusionError::NotImplemented(format!(
            "Hive TextFile line delimiter {:?} is not supported",
            serde_properties.line_delimiter as char
        )));
    }
    // The CSV reader only honors escapes inside quoted fields, which Hive
    // TextFile does not have, so escaped delimiters cannot be read correctly.
    if serde_properties.escape_delimiter.is_some() {
        return Err(DataFusionError::NotImplemented(
            "Hive TextFile tables with escape.delim are not supported".to_string(),
        ));
    }
    if serde_properties.skip_header_line_count > 1 {
        return Err(DataFusionError::NotImplemented(format!(
            "Hive TextFile skip.header.line.count={} is not supported",
            serde_properties.skip_header_line_count
        )));
    }
    // A CSV scan decompresses all of its files with a single codec.
    let compressions = file_group
        .files()
        .iter()
        .map(|file| detect_file_compression(&file.object_meta.location))
        .collect::<Result<HashSet<_>>>()?;
    if compressions.len() > 1 {
        return Err(DataFusionError::NotImplemented(
            "mixed compression in hive textfile scan is not supported".to_string(),
        ));
    }
    let compression = compressions
        .into_iter()
        .next()
        .unwrap_or(CompressionTypeVariant::UNCOMPRESSED);

    Ok(CsvOptions {
        has_header: Some(serde_properties.skip_header_line_count == 1),
        delimiter: serde_properties.field_delimiter,
        // LazySimpleSerDe has no quoting; NUL effectively disables it.
        quote: b'\0',
        // Hive fills missing trailing columns with NULL.
        truncated_rows: Some(true),
        compression,
        ..Default::default()
    })
}

fn try_fill_table_statistics_by_file_list(
    mut statistics: Statistics,
    table_schema: SchemaRef,
    files: &[PartitionedFile],
    partition_pruned: bool,
) -> Statistics {
    // Catalog-level num_rows/total_byte_size describe the whole table; once
    // partitions are pruned they overestimate the scan, so re-estimate both
    // from the surviving files instead.
    if partition_pruned {
        statistics.num_rows = Precision::Absent;
        statistics.total_byte_size = Precision::Absent;
    }
    let total_size = files
        .iter()
        .map(|file| usize::try_from(file.object_meta.size).unwrap_or(usize::MAX))
        .fold(0usize, usize::saturating_add);

    if matches!(statistics.num_rows, Precision::Absent) {
        statistics.num_rows = Precision::Inexact(estimate_num_rows(&table_schema, total_size));
    }
    if matches!(statistics.total_byte_size, Precision::Absent) {
        statistics.total_byte_size = Precision::Inexact(total_size);
    }
    statistics
}

fn estimate_num_rows(schema: &Schema, total_size: usize) -> usize {
    if total_size == 0 {
        return 0;
    }

    let row_size = estimate_schema_row_size(schema);
    total_size.saturating_div(row_size).max(1)
}

fn estimate_schema_row_size(schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .map(|field| estimate_data_type_size(field.data_type()))
        .fold(0usize, usize::saturating_add)
        .max(1)
}

fn estimate_data_type_size(data_type: &DataType) -> usize {
    const VARIABLE_TYPE_SIZE: usize = 16;

    data_type.primitive_width().unwrap_or(VARIABLE_TYPE_SIZE)
}

fn build_partition_values(
    partition_fields: &[Arc<datafusion::arrow::datatypes::Field>],
    partition: &HivePartition,
) -> Result<Vec<ScalarValue>> {
    partition_fields
        .iter()
        .zip(partition.partition_values.iter())
        .map(|(field, val)| parse_partition_value(val, field.data_type()))
        .collect()
}

/// Detects the compression codec of a Hive TextFile file from its extension.
fn detect_file_compression(location: &Path) -> Result<CompressionTypeVariant> {
    let file_name = location.filename().unwrap_or("");
    if file_name.ends_with(".gz") || file_name.ends_with(".gzip") {
        Ok(CompressionTypeVariant::GZIP)
    } else if file_name.ends_with(".bz2") {
        Ok(CompressionTypeVariant::BZIP2)
    } else if file_name.ends_with(".xz") {
        Ok(CompressionTypeVariant::XZ)
    } else if file_name.ends_with(".zst") || file_name.ends_with(".zstd") {
        Ok(CompressionTypeVariant::ZSTD)
    } else if let Some(codec) = [".snappy", ".deflate", ".lzo", ".lzo_deflate", ".lz4"]
        .into_iter()
        .find(|extension| file_name.ends_with(extension))
    {
        // Reading these as plain text would silently return garbage.
        Err(DataFusionError::NotImplemented(format!(
            "{} compressed Hive TextFile files are not supported: {location}",
            &codec[1..],
        )))
    } else {
        Ok(CompressionTypeVariant::UNCOMPRESSED)
    }
}

// The parquet scan builder mirrors DataFusion scan inputs, so keeping these
// arguments explicit is clearer than wrapping them only to satisfy clippy.
#[allow(clippy::too_many_arguments)]
fn build_parquet_exec(
    io_handle: Handle,
    store_url: ObjectStoreUrl,
    table_schema: TableSchema,
    file_group: FileGroup,
    state: &dyn Session,
    statistics: Statistics,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut parquet_options = TableParquetOptions {
        global: state.config_options().execution.parquet.clone(),
        ..Default::default()
    };
    parquet_options.global.pushdown_filters = true;
    parquet_options.global.reorder_filters = true;
    // Hive timestamps are microsecond columns. Decoding INT96 straight to
    // microseconds also avoids the nanosecond range overflow (years outside
    // 1677-2262) that coercing through nanoseconds would hit.
    if parquet_options.global.coerce_int96.is_none() {
        parquet_options.global.coerce_int96 = Some("us".to_string());
    }

    let store = state.runtime_env().object_store(&store_url)?;

    let reader_options = ExtendedParquetReaderOptions {
        metadata_cache: Some(state.runtime_env().cache_manager.get_file_metadata_cache()),
    };
    let parquet_file_reader_factory = Arc::new(ExtendedParquetFileReaderFactory::new(
        store.clone(),
        io_handle,
        reader_options,
    ));

    let mut source = ParquetSource::new(table_schema);
    if let Some(hint) = parquet_options.global.metadata_size_hint {
        source = source.with_metadata_size_hint(hint);
    };
    source = source
        .with_table_parquet_options(parquet_options)
        .with_parquet_file_reader_factory(parquet_file_reader_factory);

    let mut builder = FileScanConfigBuilder::new(store_url, Arc::new(source))
        .with_file_group(file_group)
        .with_expr_adapter(Some(Arc::new(CaseInsensitivePhysicalExprAdapterFactory)));
    builder = builder.with_statistics(statistics);
    if let Some(proj) = projection {
        builder = builder.with_projection_indices(Some(proj.clone()))?;
    }
    if let Some(lim) = limit {
        builder = builder.with_limit(Some(lim));
    }
    let config = builder.build();
    Ok(DataSourceExec::from_data_source(config))
}

fn prune_partitions(
    partitions: &[HivePartition],
    partition_fields: &[Arc<Field>],
    filters: &[Expr],
    state: &dyn Session,
) -> Result<Vec<usize>> {
    if partition_fields.is_empty() {
        return Err(DataFusionError::Internal(
            "partition fields is empty".to_string(),
        ));
    }
    if filters.is_empty() {
        return Ok((0..partitions.len()).collect());
    }

    let partition_schema = Arc::new(Schema::new(
        partition_fields
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));

    let partition_col_name_set: HashSet<String> = partition_schema
        .fields()
        .iter()
        .map(|field| field.name().to_ascii_lowercase())
        .collect();

    let partition_filters: Vec<&Expr> = filters
        .iter()
        .filter(|f| {
            !f.column_refs().is_empty()
                && f.column_refs()
                    .iter()
                    .all(|c| partition_col_name_set.contains(&c.name().to_ascii_lowercase()))
        })
        .collect();

    if partition_filters.is_empty() {
        return Ok((0..partitions.len()).collect());
    }

    let df_schema = partition_schema.as_ref().clone().to_dfschema()?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(partition_fields.len());
    for (field_idx, field) in partition_fields.iter().enumerate() {
        let values: Vec<Option<&str>> = partitions
            .iter()
            .map(|partition| {
                partition
                    .partition_values
                    .get(field_idx)
                    .map(|value| value.as_str())
            })
            .collect();
        columns.push(build_partition_array(field.data_type(), &values)?);
    }

    let batch = RecordBatch::try_new(partition_schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let compiled_filters = partition_filters
        .into_iter()
        .map(|filter| state.create_physical_expr(filter.clone(), &df_schema))
        .collect::<Result<Vec<_>>>()?;

    let mut filter_result: Option<BooleanArray> = None;
    for filter in compiled_filters {
        let result_array = filter
            .evaluate(&batch)?
            .to_array_of_size(batch.num_rows())?;
        let bool_array = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("partition filter did not produce boolean array".into())
            })?;
        filter_result = Some(match filter_result {
            None => bool_array.clone(),
            Some(acc) => compute::and(&acc, bool_array)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        });
    }

    let mut surviving = Vec::new();
    match filter_result {
        None => surviving.extend(0..partitions.len()),
        Some(combined_array) => {
            for row_idx in 0..partitions.len() {
                if !combined_array.is_null(row_idx) && combined_array.value(row_idx) {
                    surviving.push(row_idx);
                }
            }
        }
    }

    Ok(surviving)
}

fn build_partition_array(data_type: &DataType, values: &[Option<&str>]) -> Result<ArrayRef> {
    let normalized_values: Vec<Option<&str>> = values
        .iter()
        .map(|value| normalize_partition_value(*value))
        .collect();

    match data_type {
        DataType::Int8 => {
            let arr = Int8Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i8>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int16 => {
            let arr = Int16Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i16>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int32 => {
            let arr = Int32Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i32>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int64 => {
            let arr = Int64Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Float32 => {
            let arr = Float32Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<f32>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Float64 => {
            let arr = Float64Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<f64>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Boolean => {
            let arr = BooleanArray::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<bool>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Date32 => {
            let arr = Date32Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(parse_date_to_days))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = TimestampMicrosecondArray::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(parse_timestamp_micros))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Utf8 => {
            let arr = StringArray::from(normalized_values);
            Ok(Arc::new(arr) as ArrayRef)
        }
        // e.g. decimal partitions; values that do not parse become NULL.
        _ => Ok(compute::cast(
            &StringArray::from(normalized_values),
            data_type,
        )?),
    }
}

fn normalize_partition_value(value: Option<&str>) -> Option<&str> {
    match value {
        Some("__HIVE_DEFAULT_PARTITION__") | Some("") => None,
        _ => value,
    }
}

/// Parse a date string like "2024-01-15" into days since epoch (Date32).
fn parse_date_to_days(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year = parts[0].parse::<i32>().ok()?;
    let month = parts[1].parse::<u32>().ok()?;
    let day = parts[2].parse::<u32>().ok()?;
    days_since_epoch(year, month, day)
}

/// Compute days since 1970-01-01 using the proleptic Gregorian calendar.
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days_in_months = [0u32, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let leap = is_leap_year(year);
    let feb_days = if leap { 29u32 } else { 28u32 };

    let mut day_of_year: i64 = 0;
    for m in 1..month {
        day_of_year += if m == 2 {
            feb_days
        } else {
            days_in_months[m as usize]
        } as i64;
    }
    day_of_year += day as i64 - 1;

    let epoch_year = 1970i32;
    let mut total_days: i64 = 0;
    if year >= epoch_year {
        for y in epoch_year..year {
            total_days += if is_leap_year(y) { 366 } else { 365 };
        }
    } else {
        for y in year..epoch_year {
            total_days -= if is_leap_year(y) { 366 } else { 365 };
        }
    }
    total_days += day_of_year;
    Some(total_days as i32)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Parse a timestamp string like "2024-01-15 12:34:56" or "2024-01-15T12:34:56"
/// into microseconds since epoch.
fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let s = s.replace('T', " ");
    let parts: Vec<&str> = s.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return None;
    }
    let days = parse_date_to_days(parts[0])? as i64;
    let time_parts: Vec<&str> = parts[1].splitn(3, ':').collect();
    if time_parts.len() < 2 {
        return None;
    }
    let hour = time_parts[0].parse::<i64>().ok()?;
    let minute = time_parts[1].parse::<i64>().ok()?;
    let seconds_str = if time_parts.len() == 3 {
        time_parts[2]
    } else {
        "0"
    };
    let sec_parts: Vec<&str> = seconds_str.splitn(2, '.').collect();
    let seconds = sec_parts[0].parse::<i64>().ok()?;
    let micros_frac = if sec_parts.len() == 2 {
        let frac = sec_parts[1];
        let padded = format!("{:0<6}", frac);
        padded[..6].parse::<i64>().ok()?
    } else {
        0
    };

    let micros = days * 86_400_000_000
        + hour * 3_600_000_000
        + minute * 60_000_000
        + seconds * 1_000_000
        + micros_frac;
    Some(micros)
}

fn parse_partition_value(s: &str, data_type: &DataType) -> Result<ScalarValue> {
    use datafusion::scalar::ScalarValue;

    if s == "__HIVE_DEFAULT_PARTITION__" || s.is_empty() {
        return ScalarValue::try_from(data_type);
    }

    match data_type {
        DataType::Int8 => s
            .parse::<i8>()
            .map(|v| ScalarValue::Int8(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Int16 => s
            .parse::<i16>()
            .map(|v| ScalarValue::Int16(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Int32 => s
            .parse::<i32>()
            .map(|v| ScalarValue::Int32(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Int64 => s
            .parse::<i64>()
            .map(|v| ScalarValue::Int64(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Float32 => s
            .parse::<f32>()
            .map(|v| ScalarValue::Float32(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Float64 => s
            .parse::<f64>()
            .map(|v| ScalarValue::Float64(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Boolean => s
            .parse::<bool>()
            .map(|v| ScalarValue::Boolean(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(s.to_string()))),
        DataType::Date32 => parse_date_to_days(s)
            .map(|v| ScalarValue::Date32(Some(v)))
            .ok_or_else(|| DataFusionError::Internal(format!("failed to parse date: {}", s))),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => parse_timestamp_micros(s)
            .map(|v| ScalarValue::TimestampMicrosecond(Some(v), tz.clone()))
            .ok_or_else(|| DataFusionError::Internal(format!("failed to parse timestamp: {}", s))),
        // e.g. decimal partitions
        _ => ScalarValue::Utf8(Some(s.to_string())).cast_to(data_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_format::hive::hive_type::hive_type_to_arrow_type;
    use datafusion::arrow::datatypes::Field;
    use datafusion::assert_batches_eq;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{Expr, Operator, binary_expr, col, lit};
    use datafusion::object_store::ObjectStoreExt;
    use datafusion::object_store::memory::InMemory;
    use datafusion::prelude::SessionContext;
    use url::Url;

    #[test]
    fn test_try_fill_table_statistics_estimates_rows_when_num_rows_missing() {
        let table_schema = statistics_schema();
        let files = vec![
            PartitionedFile::new("file1.parquet", 64),
            PartitionedFile::new("file2.parquet", 56),
        ];

        let statistics = try_fill_table_statistics_by_file_list(
            Statistics::new_unknown(&table_schema),
            table_schema,
            &files,
            false,
        );

        assert_eq!(statistics.total_byte_size, Precision::Inexact(120));
        assert_eq!(statistics.num_rows, Precision::Inexact(10));
    }

    #[test]
    fn test_try_fill_table_statistics_preserves_existing_values() {
        let table_schema = statistics_schema();
        let files = vec![PartitionedFile::new("file1.parquet", 64)];

        let mut complete = Statistics::new_unknown(&table_schema);
        complete.num_rows = Precision::Inexact(7);
        complete.total_byte_size = Precision::Exact(128);
        complete.column_statistics[0].distinct_count = Precision::Inexact(5);
        let statistics =
            try_fill_table_statistics_by_file_list(complete, table_schema.clone(), &files, false);
        assert_eq!(statistics.num_rows, Precision::Inexact(7));
        assert_eq!(statistics.total_byte_size, Precision::Exact(128));
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Inexact(5)
        );

        let mut existing_num_rows = Statistics::new_unknown(&table_schema);
        existing_num_rows.num_rows = Precision::Inexact(7);
        let statistics = try_fill_table_statistics_by_file_list(
            existing_num_rows,
            table_schema.clone(),
            &files,
            false,
        );
        assert_eq!(statistics.num_rows, Precision::Inexact(7));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(64));

        let mut existing_total_byte_size = Statistics::new_unknown(&table_schema);
        existing_total_byte_size.total_byte_size = Precision::Exact(128);
        let statistics = try_fill_table_statistics_by_file_list(
            existing_total_byte_size,
            table_schema,
            &files,
            false,
        );
        assert_eq!(statistics.num_rows, Precision::Inexact(5));
        assert_eq!(statistics.total_byte_size, Precision::Exact(128));
    }

    #[test]
    fn test_try_fill_table_statistics_reestimates_when_partitions_pruned() {
        let table_schema = statistics_schema();
        let files = vec![PartitionedFile::new("file1.parquet", 64)];

        let mut catalog_statistics = Statistics::new_unknown(&table_schema);
        catalog_statistics.num_rows = Precision::Inexact(1000);
        catalog_statistics.total_byte_size = Precision::Inexact(4096);
        catalog_statistics.column_statistics[0].distinct_count = Precision::Inexact(5);

        let statistics = try_fill_table_statistics_by_file_list(
            catalog_statistics,
            table_schema.clone(),
            &files,
            true,
        );

        assert_eq!(statistics.num_rows, Precision::Inexact(5));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(64));
        assert_eq!(
            statistics.column_statistics[0].distinct_count,
            Precision::Inexact(5)
        );
    }

    #[test]
    fn test_prune_partitions_eq() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[binary_expr(col("dt"), Operator::Eq, lit("2012-01-03"))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![1, 5]);
    }

    #[test]
    fn test_prune_partitions_in_list_and_gt() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let in_list = Expr::InList(InList::new(
            Box::new(col("dt")),
            vec![lit("2012-01-01"), lit("2012-01-04")],
            false,
        ));
        let gt = binary_expr(col("dt"), Operator::Gt, lit("2012-01-01"));

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[in_list, gt],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![2]);
    }

    #[test]
    fn test_prune_partitions_eq_on_multiple_partition_columns() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                binary_expr(col("dt"), Operator::Eq, lit("2012-01-03")),
                binary_expr(col("bucket"), Operator::Eq, lit(2_i32)),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![1]);
    }

    #[test]
    fn test_prune_partitions_ignores_mixed_data_filters() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                binary_expr(col("dt"), Operator::Eq, lit("2012-01-03")),
                binary_expr(col("c1"), Operator::Gt, lit(10_i32)),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![1, 5]);
    }

    #[test]
    fn test_prune_partitions_without_partition_filter_keeps_all() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[binary_expr(col("c1"), Operator::Gt, lit(10_i32))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_prune_partitions_null_partition_does_not_match() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[binary_expr(col("dt"), Operator::Eq, lit("2012-01-03"))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![1, 5]);
    }
    #[test]
    fn test_prune_partitions_is_null_matches_null_partitions() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[Expr::IsNull(Box::new(col("dt")))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![3, 4]);
    }

    #[test]
    fn test_prune_partitions_bucket_is_not_null_matches_non_null_partitions() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[Expr::IsNotNull(Box::new(col("bucket")))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_prune_partitions_dt_eq_and_bucket_is_null_matches_null_bucket_partition() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                binary_expr(col("dt"), Operator::Eq, lit("2012-01-03")),
                Expr::IsNull(Box::new(col("bucket"))),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![5]);
    }

    #[test]

    fn test_prune_partitions_dt_is_null_and_bucket_eq_matches_intersection() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                Expr::IsNull(Box::new(col("dt"))),
                binary_expr(col("bucket"), Operator::Eq, lit(2_i32)),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![3]);
    }

    #[test]
    fn test_prune_partitions_is_null_and_eq_matches_nothing() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                Expr::IsNull(Box::new(col("dt"))),
                binary_expr(col("dt"), Operator::Eq, lit("2012-01-03")),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, Vec::<usize>::new());
    }

    #[test]
    fn test_decimal_partition_values() {
        let data_type = DataType::Decimal128(10, 2);
        assert_eq!(
            parse_partition_value("12.5", &data_type).unwrap(),
            ScalarValue::Decimal128(Some(1250), 10, 2)
        );
        assert_eq!(
            parse_partition_value("__HIVE_DEFAULT_PARTITION__", &data_type).unwrap(),
            ScalarValue::Decimal128(None, 10, 2)
        );

        let state = SessionContext::new();
        let partition_fields = vec![Arc::new(Field::new("price", data_type, true))];
        let partitions = ["1.00", "12.50", "__HIVE_DEFAULT_PARTITION__"]
            .into_iter()
            .map(|value| HivePartition {
                location: format!("s3://warehouse/t/price={value}"),
                partition_values: vec![value.to_string()],
            })
            .collect::<Vec<_>>();
        let filter = binary_expr(
            col("price"),
            Operator::Gt,
            lit(ScalarValue::Decimal128(Some(500), 10, 2)),
        );
        let surviving =
            prune_partitions(&partitions, &partition_fields, &[filter], &state.state()).unwrap();
        assert_eq!(surviving, vec![1]);
    }

    fn partition_fields() -> Vec<Arc<Field>> {
        vec![
            Arc::new(Field::new("dt", DataType::Utf8, true)),
            Arc::new(Field::new("bucket", DataType::Int32, true)),
        ]
    }

    fn statistics_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn sample_partitions() -> Vec<HivePartition> {
        vec![
            HivePartition {
                location: "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=2012-01-01/bucket=1"
                    .to_string(),
                partition_values: vec!["2012-01-01".to_string(), "1".to_string()],
            },
            HivePartition {
                location: "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=2012-01-03/bucket=2"
                    .to_string(),
                partition_values: vec!["2012-01-03".to_string(), "2".to_string()],
            },
            HivePartition {
                location: "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=2012-01-04/bucket=3"
                    .to_string(),
                partition_values: vec!["2012-01-04".to_string(), "3".to_string()],
            },
            HivePartition {
                location:
                    "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=__HIVE_DEFAULT_PARTITION__/bucket=2"
                        .to_string(),
                partition_values: vec!["__HIVE_DEFAULT_PARTITION__".to_string(), "2".to_string()],
            },
            HivePartition {
                location: "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=/bucket=1"
                    .to_string(),
                partition_values: vec!["".to_string(), "1".to_string()],
            },
            HivePartition {
                location:
                    "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=2012-01-03/bucket=__HIVE_DEFAULT_PARTITION__"
                    .to_string(),
                partition_values: vec![
                    "2012-01-03".to_string(),
                    "__HIVE_DEFAULT_PARTITION__".to_string(),
                ],
            },
        ]
    }

    #[test]
    fn test_detect_file_compression() {
        let detect = |name: &str| detect_file_compression(&Path::from(name));
        assert_eq!(detect("a").unwrap(), CompressionTypeVariant::UNCOMPRESSED);
        assert_eq!(
            detect("b.txt").unwrap(),
            CompressionTypeVariant::UNCOMPRESSED
        );
        assert_eq!(detect("a.gz").unwrap(), CompressionTypeVariant::GZIP);
        assert_eq!(detect("a.bz2").unwrap(), CompressionTypeVariant::BZIP2);
        let err = detect("000000_0.snappy").unwrap_err();
        assert!(err.to_string().contains("snappy"), "{err}");
        assert!(detect("000000_0.lzo").is_err());
        assert!(detect("000000_0.deflate").is_err());
    }

    #[tokio::test]
    async fn test_scan_textfile_default_serde() -> Result<()> {
        // Hive's default TextFile layout: \x01 delimiter declared through
        // serialization.format=1 and \N for NULL.
        let batches = scan_hive_table(
            text_file(&[("serialization.format", "1")]),
            text_fields(),
            &[(
                "hive/table/000000_0",
                // Row 4 has values that do not parse; Hive reads them as NULL.
                "1\x01alice\x0110.5\n2\x01\\N\x01\\N\n3\x01\x01\nx\x01bob\x01abc\n",
            )],
            "SELECT id, name, amount FROM t ORDER BY id, name",
        )
        .await?;
        assert_batches_eq!(
            [
                "+----+-------+--------+",
                "| id | name  | amount |",
                "+----+-------+--------+",
                "| 1  | alice | 10.5   |",
                "| 2  |       |        |",
                "| 3  |       |        |",
                "|    | bob   |        |",
                "+----+-------+--------+",
            ],
            &batches
        );
        let batches = scan_hive_table(
            text_file(&[("serialization.format", "1")]),
            text_fields(),
            &[("hive/table/000000_0", "2\x01\\N\x01\\N\n")],
            "SELECT count(name), count(amount) FROM t",
        )
        .await?;
        assert_batches_eq!(
            [
                "+---------------+-----------------+",
                "| count(t.name) | count(t.amount) |",
                "+---------------+-----------------+",
                "| 0             | 0               |",
                "+---------------+-----------------+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_textfile_reads_complex_types_as_null() -> Result<()> {
        let fields = hive_fields(&[
            ("id", "int"),
            ("tags", "array<string>"),
            ("props", "map<string,int>"),
            ("s", "struct<a:int>"),
            ("u", "uniontype<int,string>"),
        ]);
        let files = [(
            "hive/table/000000_0",
            "1\x01a\x02b\x01k\x031\x015\x01text\n",
        )];
        let batches = scan_hive_table(
            text_file(&[]),
            fields.clone(),
            &files,
            "SELECT id, tags, props, s FROM t",
        )
        .await?;
        assert_eq!(
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
            fields[..4]
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>()
        );
        assert_batches_eq!(
            [
                "+----+------+-------+---+",
                "| id | tags | props | s |",
                "+----+------+-------+---+",
                "| 1  |      |       |   |",
                "+----+------+-------+---+",
            ],
            &batches
        );

        let description =
            scan_hive_table(text_file(&[]), fields.clone(), &files, "DESCRIBE t").await?;
        let described_types = description[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(described_types.value(1), "List(Utf8, field: 'element')");
        assert_eq!(
            described_types.value(2),
            "Map(\"key_value\": non-null Struct(\"key\": non-null Utf8, \"value\": Int32), unsorted)"
        );
        assert_eq!(described_types.value(3), "Struct(\"a\": Int32)");
        assert_eq!(
            described_types.value(4),
            "Union(Sparse, 0: (\"_union_0\": Int32), 1: (\"_union_1\": Utf8))"
        );

        let union =
            scan_hive_table(text_file(&[]), fields.clone(), &files, "SELECT u FROM t").await?;
        assert_eq!(
            union[0].schema().field(0).data_type(),
            fields[4].data_type()
        );
        // Union arrays have no top-level validity bitmap.
        assert!(union[0].column(0).logical_nulls().unwrap().is_null(0));

        let batches = scan_hive_table(
            text_file(&[]),
            fields,
            &files,
            "SELECT id FROM t WHERE tags IS NULL AND s IS NULL AND u IS NULL",
        )
        .await?;
        assert_batches_eq!(["+----+", "| id |", "+----+", "| 1  |", "+----+"], &batches);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_textfile_custom_serde_properties() -> Result<()> {
        // Tab delimiter, custom null format, a header line, no quoting and a
        // row with missing trailing columns.
        let batches = scan_hive_table(
            text_file(&[
                ("field.delim", "\t"),
                ("serialization.null.format", "NULL"),
                ("skip.header.line.count", "1"),
            ]),
            text_fields(),
            &[(
                "hive/table/000000_0",
                "id\tname\tamount\n1\t\"quoted\tNULL\n2\tNULL\t2.5\n3\n",
            )],
            "SELECT id, name, amount FROM t ORDER BY id",
        )
        .await?;
        assert_batches_eq!(
            [
                "+----+---------+--------+",
                "| id | name    | amount |",
                "+----+---------+--------+",
                "| 1  | \"quoted |        |",
                "| 2  |         | 2.5    |",
                "| 3  |         |        |",
                "+----+---------+--------+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_textfile_rejects_unsupported_serde_properties() {
        for (key, value) in [
            ("escape.delim", "\\"),
            ("skip.header.line.count", "2"),
            ("line.delim", "\r"),
        ] {
            let err = scan_hive_table(
                text_file(&[(key, value)]),
                text_fields(),
                &[("hive/table/000000_0", "1\x01a\x011.0\n")],
                "SELECT * FROM t",
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("not supported"), "{key}: {err}");
        }

        let err = scan_hive_table(
            text_file(&[]),
            text_fields(),
            &[
                ("hive/table/000000_0.gz", "x"),
                ("hive/table/000001_0.bz2", "x"),
            ],
            "SELECT * FROM t",
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("mixed compression"), "{err}");
    }

    #[tokio::test]
    async fn test_scan_partitioned_textfile() -> Result<()> {
        let partition = |dt: &str| HivePartition {
            location: format!("s3://warehouse/hive/table/dt={dt}"),
            partition_values: vec![dt.to_string()],
        };
        let files = [
            (
                "hive/table/dt=2024-01-01/000000_0",
                b"1\x01a\x011.5\n".to_vec(),
            ),
            (
                "hive/table/dt=2024-01-02/000000_0",
                b"2\x01b\x01\\N\n".to_vec(),
            ),
        ];
        let scan = |sql: &'static str| {
            scan_hive_table_bytes(
                HiveInputFormat::TextFile(Default::default()),
                text_fields(),
                vec![Field::new("dt", DataType::Date32, true)],
                vec![partition("2024-01-01"), partition("2024-01-02")],
                &files,
                sql,
            )
        };

        let batches = scan("SELECT dt, amount, id FROM t ORDER BY id").await?;
        assert_batches_eq!(
            [
                "+------------+--------+----+",
                "| dt         | amount | id |",
                "+------------+--------+----+",
                "| 2024-01-01 | 1.5    | 1  |",
                "| 2024-01-02 |        | 2  |",
                "+------------+--------+----+",
            ],
            &batches
        );
        let batches =
            scan("SELECT dt, count(*) AS c FROM t WHERE dt = DATE '2024-01-02' GROUP BY dt")
                .await?;
        assert_batches_eq!(
            [
                "+------------+---+",
                "| dt         | c |",
                "+------------+---+",
                "| 2024-01-02 | 1 |",
                "+------------+---+",
            ],
            &batches
        );
        let batches = scan("SELECT count(*) AS c FROM t").await?;
        assert_batches_eq!(["+---+", "| c |", "+---+", "| 2 |", "+---+"], &batches);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_parquet_matches_columns_case_insensitively() -> Result<()> {
        // Hive and Glue lowercase column names, while Spark keeps the original
        // case in the parquet files it writes.
        let batch = RecordBatch::try_from_iter([
            ("userId", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "EventName",
                Arc::new(StringArray::from(vec!["open", "close"])) as ArrayRef,
            ),
        ])?;
        let batches = scan_hive_table_bytes(
            HiveInputFormat::Parquet,
            vec![
                Field::new("userid", DataType::Int64, true),
                Field::new("eventname", DataType::Utf8, true),
            ],
            vec![],
            vec![],
            &[("hive/table/part-0.parquet", write_parquet(&batch))],
            "SELECT userid, eventname FROM t WHERE userid > 1",
        )
        .await?;
        assert_batches_eq!(
            [
                "+--------+-----------+",
                "| userid | eventname |",
                "+--------+-----------+",
                "| 2      | close     |",
                "+--------+-----------+",
            ],
            &batches
        );
        Ok(())
    }

    fn hive_fields(columns: &[(&str, &str)]) -> Vec<Field> {
        columns
            .iter()
            .map(|(name, hive_type)| {
                Field::new(*name, hive_type_to_arrow_type(hive_type).unwrap(), true)
            })
            .collect()
    }

    #[tokio::test]
    async fn test_scan_parquet_nested_types() -> Result<()> {
        use datafusion::arrow::array::{
            Int32Builder, ListBuilder, MapBuilder, StringBuilder, StructArray,
        };
        use datafusion::arrow::buffer::NullBuffer;

        // arrow-rs names the children `item`, `entries`, `keys` and `values`,
        // unlike the table types, and the struct field keeps Spark's casing.
        let mut arr = ListBuilder::new(Int32Builder::new());
        arr.append_value([Some(1), Some(2)]);
        arr.append_null();
        let mut m = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        m.keys().append_value("k");
        m.values().append_value(10);
        m.append(true).unwrap();
        m.append(false).unwrap();
        let s = StructArray::new(
            vec![Arc::new(Field::new("userId", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![7, 8]))],
            Some(NullBuffer::from(vec![true, false])),
        );
        let item = StructArray::new(
            vec![Arc::new(Field::new("Name", DataType::Utf8, true))].into(),
            vec![Arc::new(StringArray::from(vec!["x", "y"]))],
            None,
        );
        let items = datafusion::arrow::array::ListArray::new(
            Arc::new(Field::new("item", item.data_type().clone(), true)),
            datafusion::arrow::buffer::OffsetBuffer::from_lengths([2, 0]),
            Arc::new(item),
            None,
        );
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            ("arr", Arc::new(arr.finish()) as ArrayRef),
            ("m", Arc::new(m.finish()) as ArrayRef),
            ("s", Arc::new(s) as ArrayRef),
            ("items", Arc::new(items) as ArrayRef),
        ])?;
        let batches = scan_hive_table_bytes(
            HiveInputFormat::Parquet,
            hive_fields(&[
                ("id", "int"),
                ("arr", "array<int>"),
                ("m", "map<string,int>"),
                // `extra` is not in the file.
                ("s", "struct<userid:int,extra:string>"),
                ("items", "array<struct<name:string>>"),
            ]),
            vec![],
            vec![],
            &[("hive/table/part-0.parquet", write_parquet(&batch))],
            "SELECT id, arr, arr[2] AS second, m['k'] AS k, s, s['userid'] AS userid, \
             items[1]['name'] AS first_name FROM t ORDER BY id",
        )
        .await?;
        assert_batches_eq!(
            [
                "+----+--------+--------+----+----------------------+--------+------------+",
                "| id | arr    | second | k  | s                    | userid | first_name |",
                "+----+--------+--------+----+----------------------+--------+------------+",
                "| 1  | [1, 2] | 2      | 10 | {userid: 7, extra: } | 7      | x          |",
                "| 2  |        |        |    |                      |        |            |",
                "+----+--------+--------+----+----------------------+--------+------------+",
            ],
            &batches
        );

        // Filters on nested columns are pushed into the parquet scan, which
        // must still evaluate them after the columns are converted.
        for (struct_type, filter, expected) in [
            (
                "struct<userid:int,extra:string>",
                "s['userid'] = 7",
                vec![1],
            ),
            ("struct<userid:bigint>", "s['userid'] = 7", vec![1]),
            (
                "struct<userid:int,extra:string>",
                "s['extra'] IS NULL",
                vec![1, 2],
            ),
            ("struct<userid:int>", "s IS NOT NULL", vec![1]),
            ("struct<userid:int>", "array_has(arr, 2)", vec![1]),
        ] {
            let batches = scan_hive_table_bytes(
                HiveInputFormat::Parquet,
                hive_fields(&[("id", "int"), ("arr", "array<int>"), ("s", struct_type)]),
                vec![],
                vec![],
                &[("hive/table/part-0.parquet", write_parquet(&batch))],
                &format!("SELECT id FROM t WHERE {filter} ORDER BY id"),
            )
            .await?;
            let ids: Vec<i32> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(ids, expected, "{struct_type} WHERE {filter}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_parquet_legacy_two_level_list() -> Result<()> {
        use datafusion::parquet::data_type::Int32Type;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::parser::parse_message_type;

        // Hive and Spark's legacy format write lists without the middle
        // repeated group.
        let schema = Arc::new(
            parse_message_type(
                "message hive_schema {
                    required int32 id;
                    optional group arr (LIST) { repeated int32 array; }
                }",
            )
            .unwrap(),
        );
        let mut buffer = Vec::new();
        let mut writer = SerializedFileWriter::new(
            &mut buffer,
            schema,
            Arc::new(WriterProperties::builder().build()),
        )?;
        let mut row_group = writer.next_row_group()?;
        let mut column = row_group.next_column()?.unwrap();
        column
            .typed::<Int32Type>()
            .write_batch(&[1, 2, 3], None, None)?;
        column.close()?;
        // Rows: [1, 2], NULL, [].
        let mut column = row_group.next_column()?.unwrap();
        column.typed::<Int32Type>().write_batch(
            &[1, 2],
            Some(&[2, 2, 0, 1]),
            Some(&[0, 1, 0, 0]),
        )?;
        column.close()?;
        row_group.close()?;
        writer.close()?;

        let batches = scan_hive_table_bytes(
            HiveInputFormat::Parquet,
            hive_fields(&[("id", "int"), ("arr", "array<bigint>")]),
            vec![],
            vec![],
            &[("hive/table/000000_0", buffer)],
            "SELECT id, arr, cardinality(arr) AS n FROM t ORDER BY id",
        )
        .await?;
        assert_batches_eq!(
            [
                "+----+--------+---+",
                "| id | arr    | n |",
                "+----+--------+---+",
                "| 1  | [1, 2] | 2 |",
                "| 2  |        |   |",
                "| 3  | []     | 0 |",
                "+----+--------+---+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_parquet_int96_timestamps() -> Result<()> {
        use datafusion::parquet::data_type::{Int32Type, Int96, Int96Type};
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::parser::parse_message_type;

        // Hive, Impala and older Spark write timestamps as INT96: nanoseconds
        // of the day followed by the Julian day number.
        let schema = Arc::new(
            parse_message_type("message hive { required int32 id; required int96 ts; }").unwrap(),
        );
        let mut buffer = Vec::new();
        let mut writer = SerializedFileWriter::new(
            &mut buffer,
            schema,
            Arc::new(WriterProperties::builder().build()),
        )?;
        let mut row_group = writer.next_row_group()?;
        let mut column = row_group.next_column()?.unwrap();
        column
            .typed::<Int32Type>()
            .write_batch(&[1, 2], None, None)?;
        column.close()?;
        let mut column = row_group.next_column()?.unwrap();
        let nanos_of_day: u64 = (12 * 3600 + 34 * 60 + 56) * 1_000_000_000 + 123_456_000;
        column.typed::<Int96Type>().write_batch(
            &[
                // 1970-01-02 12:34:56.123456
                Int96::from(vec![
                    nanos_of_day as u32,
                    (nanos_of_day >> 32) as u32,
                    2_440_589,
                ]),
                // 2500-01-01, outside the nanosecond timestamp range
                Int96::from(vec![0, 0, 2_634_167]),
            ],
            None,
            None,
        )?;
        column.close()?;
        row_group.close()?;
        writer.close()?;

        let batches = scan_hive_table_bytes(
            HiveInputFormat::Parquet,
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            ],
            vec![],
            vec![],
            &[("hive/table/part-0.parquet", buffer)],
            "SELECT id, ts FROM t ORDER BY id",
        )
        .await?;
        assert_batches_eq!(
            [
                "+----+----------------------------+",
                "| id | ts                         |",
                "+----+----------------------------+",
                "| 1  | 1970-01-02T12:34:56.123456 |",
                "| 2  | 2500-01-01T00:00:00        |",
                "+----+----------------------------+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_parquet_prunes_row_groups_with_filters() -> Result<()> {
        // Filters reported as inexact are pushed into the parquet scan by
        // DataFusion's physical filter pushdown, which prunes row groups.
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from((0..100).collect::<Vec<_>>())) as ArrayRef,
        )])?;
        let mut buffer = Vec::new();
        let properties = datafusion::parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
            &mut buffer,
            batch.schema(),
            Some(properties),
        )?;
        writer.write(&batch)?;
        writer.close()?;

        let batches = scan_hive_table_bytes(
            HiveInputFormat::Parquet,
            vec![Field::new("id", DataType::Int64, true)],
            vec![],
            vec![],
            &[("hive/table/part-0.parquet", buffer)],
            "EXPLAIN ANALYZE SELECT id FROM t WHERE id = 42",
        )
        .await?;
        let plan = datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
        assert!(
            plan.contains("row_groups_pruned_statistics=10 total \u{2192} 1 matched"),
            "{plan}"
        );
        Ok(())
    }

    fn write_parquet(batch: &RecordBatch) -> Vec<u8> {
        let mut buffer = Vec::new();
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None)
                .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buffer
    }

    fn text_file(serde_properties: &[(&str, &str)]) -> HiveInputFormat {
        HiveInputFormat::TextFile(
            TextFileSerdeProperties::try_new(
                &serde_properties
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn text_fields() -> Vec<Field> {
        vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]
    }

    /// Scans an unpartitioned Hive table at `s3://warehouse/hive/table` backed
    /// by an in-memory object store and runs `sql` against it as table `t`.
    async fn scan_hive_table(
        input_format: HiveInputFormat,
        fields: Vec<Field>,
        files: &[(&str, &str)],
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        scan_hive_table_bytes(
            input_format,
            fields,
            vec![],
            vec![],
            &files
                .iter()
                .map(|(path, data)| (*path, data.as_bytes().to_vec()))
                .collect::<Vec<_>>(),
            sql,
        )
        .await
    }

    async fn scan_hive_table_bytes(
        input_format: HiveInputFormat,
        fields: Vec<Field>,
        partition_fields: Vec<Field>,
        partitions: Vec<HivePartition>,
        files: &[(&str, Vec<u8>)],
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        let store = Arc::new(InMemory::new());
        for (path, data) in files {
            store.put(&Path::from(*path), data.clone().into()).await?;
        }
        ctx.runtime_env()
            .register_object_store(&Url::parse("s3://warehouse").unwrap(), store);

        let table_schema = TableSchema::new(
            Arc::new(Schema::new(fields)),
            partition_fields.into_iter().map(Arc::new).collect(),
        );
        let table_statistics = Statistics::new_unknown(table_schema.table_schema());
        let provider = HiveTableProvider::new(
            "s3://warehouse/hive/table".to_string(),
            HiveStorageInfo {
                input_format,
                table_schema,
                table_statistics,
            },
            partitions,
            Storage::default(),
            Handle::current(),
            String::new(),
        );
        ctx.register_table("t", Arc::new(provider))?;
        ctx.sql(sql).await?.collect().await
    }
}
