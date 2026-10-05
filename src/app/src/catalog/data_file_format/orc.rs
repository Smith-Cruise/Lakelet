// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Adapted from datafusion-contrib/datafusion-orc 0.9.0, commit
//! 3898e403dea2ea0fcfa7c13d229aaf669b9a5b24: file_source.rs,
//! physical_exec.rs and object_store_reader.rs. Changes add schema adaptation,
//! partition projections, microsecond timestamps and a dedicated I/O runtime.

use super::orc_predicate::{combine_and, predicate_columns, to_orc_predicate};
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::DataFusionError;
use datafusion::object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use orc_rust::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::AsyncChunkReader;
use orc_rust::schema::TimestampPrecision;
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug, Clone)]
pub(crate) struct OrcSource {
    metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    table_schema: TableSchema,
    projection: SplitProjection,
    io_handle: Handle,
    pruning_filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl OrcSource {
    pub(crate) fn new(table_schema: TableSchema, io_handle: Handle) -> Self {
        let projection = SplitProjection::unprojected(&table_schema);
        Self {
            metrics: ExecutionPlanMetricsSet::default(),
            batch_size: 1024,
            table_schema,
            projection,
            io_handle,
            pruning_filters: vec![],
        }
    }

    pub(crate) fn with_pruning_filters(mut self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        self.pruning_filters = filters;
        self
    }
}

impl FileSource for OrcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let opener = Arc::new(OrcOpener {
            full_logical_file_schema: Arc::clone(self.table_schema.file_schema()),
            pruning_filters: self.pruning_filters.clone(),
            logical_file_schema: Arc::new(
                self.table_schema
                    .file_schema()
                    .project(&self.projection.file_indices)?,
            ),
            batch_size: config.batch_size.unwrap_or(self.batch_size),
            object_store,
            io_handle: self.io_handle.clone(),
            expr_adapter: config
                .expr_adapter_factory
                .clone()
                .unwrap_or_else(|| Arc::new(DefaultPhysicalExprAdapterFactory)),
        });
        ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "orc"
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let projection = self.projection.source.try_merge(projection)?;
        let mut source = self.clone();
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &projection);
        Ok(Some(Arc::new(source)))
    }
}

struct OrcOpener {
    full_logical_file_schema: SchemaRef,
    pruning_filters: Vec<Arc<dyn PhysicalExpr>>,
    logical_file_schema: SchemaRef,
    batch_size: usize,
    object_store: Arc<dyn ObjectStore>,
    io_handle: Handle,
    expr_adapter: Arc<dyn PhysicalExprAdapterFactory>,
}

impl FileOpener for OrcOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let object_reader = ObjectStoreReader {
            store: Arc::clone(&self.object_store),
            file: file.object_meta,
            io_handle: self.io_handle.clone(),
        };
        let batch_size = self.batch_size;
        let logical_schema = Arc::clone(&self.logical_file_schema);
        let expr_adapter = Arc::clone(&self.expr_adapter);
        let full_logical_schema = Arc::clone(&self.full_logical_file_schema);
        let pruning_filters = self.pruning_filters.clone();

        Ok(async move {
            let mut builder = ArrowReaderBuilder::try_new_async(object_reader)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .with_batch_size(batch_size)
                .with_timestamp_precision(TimestampPrecision::Microsecond);

            let physical_schema = builder.schema();
            // Pruning expressions use the original table indices, independently
            // of any output projection pushed into this source.
            let predicate = if pruning_filters.is_empty() {
                None
            } else {
                expr_adapter
                    .create(
                        Arc::clone(&full_logical_schema),
                        Arc::clone(&physical_schema),
                    )
                    .ok()
                    .and_then(|adapter| {
                        combine_and(pruning_filters.iter().map(|filter| {
                            to_orc_predicate(
                                filter,
                                &full_logical_schema,
                                &physical_schema,
                                adapter.as_ref(),
                            )
                        }))
                    })
            };
            let projection = ProjectionExprs::from_indices(
                &(0..logical_schema.fields().len()).collect::<Vec<_>>(),
                &logical_schema,
            );
            let adapter =
                expr_adapter.create(Arc::clone(&logical_schema), Arc::clone(&physical_schema))?;
            let mut physical_indices = projection
                .clone()
                .try_map_exprs(|expr| adapter.rewrite(expr))?
                .column_indices();
            if let Some(predicate) = predicate {
                let mut columns = Vec::new();
                predicate_columns(&predicate, &mut columns);
                for column in columns {
                    physical_indices.push(physical_schema.index_of(column)?);
                }
                physical_indices.sort_unstable();
                physical_indices.dedup();
                builder = builder.with_predicate(predicate);
            }
            let root = builder.file_metadata().root_data_type();
            // ORC type IDs include nested children, unlike Arrow field indices.
            let mask = ProjectionMask::roots(
                root,
                physical_indices
                    .into_iter()
                    .map(|index| root.children()[index].data_type().column_index()),
            );
            builder = builder.with_projection(mask);
            if let Some(range) = file.range {
                builder = builder.with_file_byte_range(range.start as usize..range.end as usize);
            }

            // Selecting file columns changes their indices. Adapt again using
            // the selected physical schema, preserving logical order and names.
            let physical_schema = builder.schema();
            let adapter = expr_adapter.create(logical_schema, Arc::clone(&physical_schema))?;
            let projector = projection
                .try_map_exprs(|expr| adapter.rewrite(expr))?
                .make_projector(&physical_schema)?;
            Ok(builder
                .build_async()
                .map(move |batch| {
                    let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    projector.project_batch(&batch)
                })
                .boxed())
        }
        .boxed())
    }
}

struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    file: ObjectMeta,
    io_handle: Handle,
}

impl AsyncChunkReader for ObjectStoreReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        futures::future::ready(Ok(self.file.size)).boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let Some(end) = offset_from_start
            .checked_add(length)
            .filter(|end| *end <= self.file.size)
        else {
            return futures::future::ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ORC byte range exceeds file size",
            )))
            .boxed();
        };
        let store = Arc::clone(&self.store);
        let path = self.file.location.clone();
        let task = self.io_handle.spawn(async move {
            store
                .get_range(&path, offset_from_start..end)
                .await
                .map_err(std::io::Error::from)
        });
        async move {
            match task.await {
                Ok(result) => result,
                Err(error) => match error.try_into_panic() {
                    Ok(panic) => std::panic::resume_unwind(panic),
                    Err(error) => Err(std::io::Error::other(error)),
                },
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::common::ScalarValue;
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::listing::FileRange;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::Operator;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::projection::ProjectionExpr;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn file_range_and_partition_projection() -> Result<()> {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as datafusion::arrow::array::ArrayRef,
        )])?;
        let mut bytes = Vec::new();
        let mut writer = orc_rust::ArrowWriterBuilder::new(&mut bytes, batch.schema())
            .with_batch_size(2)
            .with_stripe_byte_size(1)
            .try_build()
            .unwrap();
        writer.write(&batch.slice(0, 2)).unwrap();
        writer.flush_stripe().unwrap();
        writer.write(&batch.slice(2, 2)).unwrap();
        writer.close().unwrap();
        let reader = ArrowReaderBuilder::try_new(Bytes::from(bytes.clone())).unwrap();
        let stripe = &reader.file_metadata().stripe_metadatas()[1];
        let offset = stripe.offset() as i64;
        let store = Arc::new(InMemory::new());
        let path = Path::from("range.orc");
        store.put(&path, bytes.into()).await?;
        let mut file = PartitionedFile::from(store.head(&path).await?);
        file.range = Some(FileRange {
            start: offset,
            end: offset + 1,
        });
        file.partition_values = vec![ScalarValue::from("selected")];
        let ctx = SessionContext::new();
        let url = ObjectStoreUrl::parse("s3://warehouse")?;
        ctx.runtime_env().register_object_store(url.as_ref(), store);
        let table_schema = TableSchema::new(
            batch.schema(),
            vec![Arc::new(Field::new("dt", DataType::Utf8, true))],
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("orc-io-test")
            .enable_all()
            .build()
            .unwrap();
        let source = Arc::new(OrcSource::new(table_schema, runtime.handle().clone()));
        let source = source
            .try_pushdown_projection(&ProjectionExprs::from_indices(
                &[1, 0],
                source.table_schema().table_schema(),
            ))?
            .unwrap();
        let source = source
            .try_pushdown_projection(&ProjectionExprs::new([
                ProjectionExpr::new(Arc::new(Column::new("dt", 0)), "partition"),
                ProjectionExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("id", 1)),
                        Operator::Plus,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                    )),
                    "next_id",
                ),
            ]))?
            .unwrap();
        let config = FileScanConfigBuilder::new(url, source)
            .with_file_group(FileGroup::new(vec![file]))
            .build();
        let plan = DataSourceExec::from_data_source(config);
        let result = collect(plan, ctx.task_ctx()).await;
        runtime.shutdown_background();
        assert_batches_eq!(
            [
                "+-----------+---------+",
                "| partition | next_id |",
                "+-----------+---------+",
                "| selected  | 4       |",
                "| selected  | 5       |",
                "+-----------+---------+",
            ],
            &result?
        );
        Ok(())
    }

    #[tokio::test]
    async fn reader_uses_known_size_and_propagates_storage_errors() -> Result<()> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("deleted.orc");
        store.put(&path, b"ORC".to_vec().into()).await?;
        let file = store.head(&path).await?;
        store.delete(&path).await?;
        let mut reader = ObjectStoreReader {
            store,
            file,
            io_handle: Handle::current(),
        };
        assert_eq!(reader.len().await.unwrap(), 3);
        assert!(reader.get_bytes(0, 3).await.is_err());
        assert_eq!(
            reader.get_bytes(u64::MAX, 1).await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        Ok(())
    }

    #[tokio::test]
    async fn predicates_prune_row_groups_before_final_projection() -> Result<()> {
        use crate::data_file_format::case_insensitive_adapter::CaseInsensitivePhysicalExprAdapterFactory;
        use datafusion::arrow::datatypes::Schema;
        use futures::TryStreamExt;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("nullable", DataType::Boolean, true),
        ]));
        let df_schema = schema.as_ref().clone().to_dfschema()?;
        let ctx = SessionContext::new();
        let state = ctx.state();
        let url = ObjectStoreUrl::parse("s3://warehouse")?;
        let fixtures: [(&str, &[u8]); 3] = [
            (
                "index",
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/data/hive-orc-pruning-index.orc"
                )),
            ),
            (
                "bloom",
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/data/hive-orc-pruning-bloom.orc"
                )),
            ),
            (
                "no-index",
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/data/hive-orc-pruning-no-index.orc"
                )),
            ),
        ];
        for (kind, bytes) in fixtures {
            let store = Arc::new(InMemory::new());
            let path = Path::from(format!("{kind}.orc"));
            store.put(&path, bytes.to_vec().into()).await?;
            let file = PartitionedFile::from(store.head(&path).await?);
            for (filter, expected_index, expected_bloom) in [
                (None, 3000, 3000),
                (Some("id = 500"), 3000, 1000),
                (Some("text = '目标'"), 3000, 1000),
                (Some("id > 1000"), 0, 0),
                (Some("flag = true"), 1000, 1000),
                (Some("flag IS NULL"), 0, 0),
                (Some("flag IS NOT NULL"), 3000, 3000),
                (Some("nullable IS NULL"), 1000, 1000),
                (Some("nullable IS NOT NULL"), 2000, 2000),
                // SDK keeps the stripe when an all-NULL group lacks typed statistics.
                (Some("nullable = true"), 3000, 3000),
                (Some("id = 500 AND length(payload) > 0"), 3000, 1000),
                (Some("id = 500 OR length(payload) > 0"), 3000, 3000),
            ] {
                let filters = filter
                    .map(|sql| {
                        state.create_physical_expr(
                            state.create_logical_expr(sql, &df_schema)?,
                            &df_schema,
                        )
                    })
                    .transpose()?
                    .into_iter()
                    .collect::<Vec<_>>();
                let mut scans = vec![(vec![3], 4096), (vec![], 4096)];
                if filter == Some("flag = true") {
                    // SDK 0.8 does not advance a select run larger than its batch
                    // size. Extra rows must remain available to the SQL filter.
                    scans.push((vec![3], 128));
                }
                for (projection, batch_size) in scans {
                    let source = Arc::new(
                        OrcSource::new(
                            TableSchema::new(Arc::clone(&schema), vec![]),
                            Handle::current(),
                        )
                        .with_pruning_filters(filters.clone()),
                    );
                    let config = FileScanConfigBuilder::new(url.clone(), source)
                        .with_batch_size(Some(batch_size))
                        .with_projection_indices(Some(projection.clone()))?
                        .with_expr_adapter(Some(Arc::new(
                            CaseInsensitivePhysicalExprAdapterFactory,
                        )))
                        .build();
                    let opener =
                        config
                            .file_source
                            .create_file_opener(store.clone(), &config, 0)?;
                    let batches: Vec<RecordBatch> =
                        opener.open(file.clone())?.await?.try_collect().await?;
                    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                    let expected = match (kind, batch_size) {
                        ("index" | "bloom", 128) => 2000,
                        ("index", _) => expected_index,
                        ("bloom", _) => expected_bloom,
                        _ => 3000,
                    };
                    assert_eq!(
                        rows, expected,
                        "{kind}: {filter:?}, projection={projection:?}"
                    );
                    assert!(
                        batches
                            .iter()
                            .all(|batch| batch.num_columns() == projection.len())
                    );
                }
            }
        }
        Ok(())
    }
}
