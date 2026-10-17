// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{decimal128_array_from_iter, to_arrow_date32};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use datafusion::arrow::array::{
    Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{LineItemGenerator, LineItemGeneratorIterator};

/// Generate  [`LineItem`]s in [`RecordBatch`] format
///
/// [`LineItem`]: tpchgen::generators::LineItem
///
// # TODOs:
// 1. create individual column iterators to avoid a copy into rows
// 2. Maybe Recycle buffers (don't reallocate new ones all the time) :thinking:
// Based off code / types from DataFusion
// https://github.com/apache/datafusion/blob/a1ae15826245097e7c12d4f0ed3425b25af6c431/benchmarks/src/tpch/mod.rs#L104-L103
pub struct LineItemArrow {
    inner: LineItemGeneratorIterator<'static>,
    batch_size: usize,
}

impl LineItemArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&LINEITEM_SCHEMA)
    }

    pub fn new(generator: LineItemGenerator<'static>) -> Self {
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

impl RecordBatchIterator for LineItemArrow {
    fn schema(&self) -> &SchemaRef {
        &LINEITEM_SCHEMA
    }
}

impl Iterator for LineItemArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    /// Generate the next batch of data, if there is one
    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        // Convert column by column
        let l_orderkey = Int64Array::from_iter_values(rows.iter().map(|row| row.l_orderkey));
        let l_partkey = Int64Array::from_iter_values(rows.iter().map(|row| row.l_partkey));
        let l_suppkey = Int64Array::from_iter_values(rows.iter().map(|row| row.l_suppkey));
        let l_linenumber = Int32Array::from_iter_values(rows.iter().map(|row| row.l_linenumber));
        let l_quantity = Decimal128Array::from_iter_values(rows.iter().map(|row| {
            // Convert the i64 to Arrow Decimal(15,2)
            // TODO it is supposed to be decimal in the spec
            (row.l_quantity as i128) * 100
        }))
        .with_precision_and_scale(15, 2)
        .unwrap();
        let l_extended_price =
            decimal128_array_from_iter(rows.iter().map(|row| row.l_extendedprice));
        let l_discount = decimal128_array_from_iter(rows.iter().map(|row| row.l_discount));
        let l_tax = decimal128_array_from_iter(rows.iter().map(|row| row.l_tax));
        let l_returnflag = StringArray::from_iter_values(rows.iter().map(|row| row.l_returnflag));
        let l_linestatus = StringArray::from_iter_values(rows.iter().map(|row| row.l_linestatus));
        let l_shipdate = Date32Array::from_iter_values(
            rows.iter().map(|row| row.l_shipdate).map(to_arrow_date32),
        );
        let l_commitdate = Date32Array::from_iter_values(
            rows.iter().map(|row| row.l_commitdate).map(to_arrow_date32),
        );
        let l_receiptdate = Date32Array::from_iter_values(
            rows.iter()
                .map(|row| row.l_receiptdate)
                .map(to_arrow_date32),
        );
        let l_shipinstruct =
            StringArray::from_iter_values(rows.iter().map(|row| row.l_shipinstruct));
        let l_shipmode = StringArray::from_iter_values(rows.iter().map(|row| row.l_shipmode));
        let l_comment = StringArray::from_iter_values(rows.iter().map(|row| row.l_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(l_orderkey),
                Arc::new(l_partkey),
                Arc::new(l_suppkey),
                Arc::new(l_linenumber),
                Arc::new(l_quantity),
                Arc::new(l_extended_price),
                Arc::new(l_discount),
                Arc::new(l_tax),
                Arc::new(l_returnflag),
                Arc::new(l_linestatus),
                Arc::new(l_shipdate),
                Arc::new(l_commitdate),
                Arc::new(l_receiptdate),
                Arc::new(l_shipinstruct),
                Arc::new(l_shipmode),
                Arc::new(l_comment),
            ],
        )
        .map_err(Into::into);

        Some(batch)
    }
}

/// Schema for the LineItem table
static LINEITEM_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_lineitem_schema);

fn make_lineitem_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Decimal128(15, 2), false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
        Field::new("l_discount", DataType::Decimal128(15, 2), false),
        Field::new("l_tax", DataType::Decimal128(15, 2), false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ]))
}
