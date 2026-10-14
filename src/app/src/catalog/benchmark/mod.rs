mod tpcds;
mod tpch;

#[cfg(test)]
mod tests;

use crate::catalog::LakeletCatalogProvider;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, Session, TableProvider};
use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkCatalogConfig {
    pub name: String,
}

const SCALES: &[(&str, f64)] = &[
    ("sf1", 1.0),
    ("sf10", 10.0),
    ("sf100", 100.0),
    ("sf300", 300.0),
    ("sf1000", 1000.0),
];

fn scale_factor(name: &str) -> Option<f64> {
    SCALES
        .iter()
        .find(|(schema, _)| *schema == name)
        .map(|(_, scale)| *scale)
}

type BatchIterator = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

#[derive(Debug, Clone, Copy)]
struct GenerationOptions {
    scale_factor: f64,
    partition: i32,
    partitions: i32,
    batch_size: usize,
}

#[derive(Debug, Clone, Copy)]
struct Table {
    name: &'static str,
    schema: fn() -> SchemaRef,
    generate: fn(GenerationOptions) -> Result<BatchIterator>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum BenchmarkKind {
    Tpch,
    Tpcds,
}

impl BenchmarkKind {
    fn tables(self) -> &'static [Table] {
        match self {
            Self::Tpch => tpch::TABLES,
            Self::Tpcds => tpcds::TABLES,
        }
    }

    fn partitions(self, table: &Table, target: usize) -> Result<i32> {
        match self {
            // The published TPCDS generators cannot reliably skip rows for
            // stateful tables and returns. Each scan owns one sequential cursor.
            Self::Tpcds => Ok(1),
            Self::Tpch if matches!(table.name, "nation" | "region") => Ok(1),
            Self::Tpch => i32::try_from(target.max(1))
                .map_err(|_| plan_datafusion_err!("Too many benchmark scan partitions: {target}")),
        }
    }
}

pub(crate) struct BenchmarkCatalog {
    kind: BenchmarkKind,
    cpu_handle: Handle,
}

impl BenchmarkCatalog {
    pub(crate) fn new(kind: BenchmarkKind, cpu_handle: Handle) -> Self {
        Self { kind, cpu_handle }
    }
}

#[async_trait]
impl LakeletCatalogProvider for BenchmarkCatalog {
    async fn list_schema_names(&self) -> Result<Vec<String>> {
        Ok(SCALES.iter().map(|(name, _)| (*name).to_string()).collect())
    }

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        if scale_factor(schema_name).is_none() {
            return plan_err!("schema {schema_name} does not exist");
        }
        Ok(self
            .kind
            .tables()
            .iter()
            .map(|table| table.name.to_string())
            .collect())
    }

    async fn schema_exist(&self, schema_name: &str) -> Result<bool> {
        Ok(scale_factor(schema_name).is_some())
    }
}

#[async_trait]
impl AsyncCatalogProvider for BenchmarkCatalog {
    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        Ok(scale_factor(name).map(|scale_factor| {
            Arc::new(BenchmarkSchema {
                kind: self.kind,
                scale_factor,
                cpu_handle: self.cpu_handle.clone(),
            }) as Arc<dyn AsyncSchemaProvider>
        }))
    }
}

struct BenchmarkSchema {
    kind: BenchmarkKind,
    scale_factor: f64,
    cpu_handle: Handle,
}

#[async_trait]
impl AsyncSchemaProvider for BenchmarkSchema {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .kind
            .tables()
            .iter()
            .find(|table| table.name == name)
            .map(|table| {
                Arc::new(BenchmarkTableProvider {
                    kind: self.kind,
                    table: *table,
                    schema: (table.schema)(),
                    scale_factor: self.scale_factor,
                    cpu_handle: self.cpu_handle.clone(),
                }) as Arc<dyn TableProvider>
            }))
    }
}

#[derive(Debug)]
struct BenchmarkTableProvider {
    kind: BenchmarkKind,
    table: Table,
    schema: SchemaRef,
    scale_factor: f64,
    cpu_handle: Handle,
}

#[async_trait]
impl TableProvider for BenchmarkTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if limit == Some(0) {
            let schema = match projection {
                Some(indices) => Arc::new(self.schema.project(indices)?),
                None => self.schema(),
            };
            return Ok(Arc::new(EmptyExec::new(schema)));
        }
        let config = state.config_options();
        let count = self
            .kind
            .partitions(&self.table, config.execution.target_partitions)?;
        let partitions = (0..count)
            .map(|partition| {
                Arc::new(BenchmarkPartitionStream {
                    table: self.table,
                    schema: self.schema(),
                    options: GenerationOptions {
                        scale_factor: self.scale_factor,
                        partition,
                        partitions: count,
                        batch_size: config.execution.batch_size.max(1),
                    },
                    cpu_handle: self.cpu_handle.clone(),
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema(),
            partitions,
            projection,
            [],
            false,
            limit,
        )?))
    }
}

#[derive(Debug)]
struct BenchmarkPartitionStream {
    table: Table,
    schema: SchemaRef,
    options: GenerationOptions,
    cpu_handle: Handle,
}

impl PartitionStream for BenchmarkPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut stream = RecordBatchReceiverStreamBuilder::new(Arc::clone(&self.schema), 1);
        let tx = stream.tx();
        let table = self.table;
        let options = self.options;
        stream.spawn_blocking_on(
            move || {
                if tx.is_closed() {
                    return Ok(());
                }
                // Initialization may allocate the shared TPCH text pool. Keep it out
                // of metadata discovery and planning, and never share mutable cursors.
                let mut generator = (table.generate)(options)?;
                while !tx.is_closed() {
                    let Some(batch) = generator.next() else { break };
                    if tx.blocking_send(Ok(batch?)).is_err() {
                        break;
                    }
                }
                Ok(())
            },
            &self.cpu_handle,
        );
        stream.build()
    }
}
