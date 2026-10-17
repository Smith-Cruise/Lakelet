// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{decimal128_array_from_iter, string_array_from_display_iter};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{PartSuppGenerator, PartSuppGeneratorIterator};

/// Generate [`PartSupp`]s in [`RecordBatch`] format
///
/// [`PartSupp`]: tpchgen::generators::PartSupp
///
pub struct PartSuppArrow {
    inner: PartSuppGeneratorIterator<'static>,
    batch_size: usize,
}

impl PartSuppArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&PARTSUPP_SCHEMA)
    }

    pub fn new(generator: PartSuppGenerator<'static>) -> Self {
        Self {
            inner: generator.iter(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for PartSuppArrow {
    fn schema(&self) -> &SchemaRef {
        &PARTSUPP_SCHEMA
    }
}

impl Iterator for PartSuppArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let ps_partkey = Int64Array::from_iter_values(rows.iter().map(|r| r.ps_partkey));
        let ps_suppkey = Int64Array::from_iter_values(rows.iter().map(|r| r.ps_suppkey));
        let ps_availqty = Int32Array::from_iter_values(rows.iter().map(|r| r.ps_availqty));
        let ps_supplycost = decimal128_array_from_iter(rows.iter().map(|r| r.ps_supplycost));
        let ps_comment = string_array_from_display_iter(rows.iter().map(|r| r.ps_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(ps_partkey),
                Arc::new(ps_suppkey),
                Arc::new(ps_availqty),
                Arc::new(ps_supplycost),
                Arc::new(ps_comment),
            ],
        )
        .map_err(Into::into);
        Some(batch)
    }
}

/// Schema for the PartSupp
static PARTSUPP_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_partsupp_schema);
fn make_partsupp_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
        Field::new("ps_comment", DataType::Utf8, false),
    ]))
}
