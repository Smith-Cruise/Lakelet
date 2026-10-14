// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{
    decimal128_array_from_iter, string_array_from_display_iter, to_arrow_date32,
};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use datafusion::arrow::array::{Date32Array, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{OrderGenerator, OrderGeneratorIterator};

/// Generate [`Order`]s in [`RecordBatch`] format
///
/// [`Order`]: tpchgen::generators::Order
///
pub struct OrderArrow {
    inner: OrderGeneratorIterator<'static>,
    batch_size: usize,
}

impl OrderArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&ORDER_SCHEMA)
    }

    pub fn new(generator: OrderGenerator<'static>) -> Self {
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

impl RecordBatchIterator for OrderArrow {
    fn schema(&self) -> &SchemaRef {
        &ORDER_SCHEMA
    }
}

impl Iterator for OrderArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let o_orderkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_orderkey));
        let o_custkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_custkey));
        let o_orderstatus = string_array_from_display_iter(rows.iter().map(|r| r.o_orderstatus));
        let o_totalprice = decimal128_array_from_iter(rows.iter().map(|r| r.o_totalprice));
        let o_orderdate =
            Date32Array::from_iter_values(rows.iter().map(|r| r.o_orderdate).map(to_arrow_date32));
        let o_orderpriority = StringArray::from_iter_values(rows.iter().map(|r| r.o_orderpriority));
        let o_clerk = string_array_from_display_iter(rows.iter().map(|r| r.o_clerk));
        let o_shippriority = Int32Array::from_iter_values(rows.iter().map(|r| r.o_shippriority));
        let o_comment = StringArray::from_iter_values(rows.iter().map(|r| r.o_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(o_orderkey),
                Arc::new(o_custkey),
                Arc::new(o_orderstatus),
                Arc::new(o_totalprice),
                Arc::new(o_orderdate),
                Arc::new(o_orderpriority),
                Arc::new(o_clerk),
                Arc::new(o_shippriority),
                Arc::new(o_comment),
            ],
        )
        .map_err(Into::into);
        Some(batch)
    }
}

/// Schema for the Order
static ORDER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_order_schema);
fn make_order_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]))
}
