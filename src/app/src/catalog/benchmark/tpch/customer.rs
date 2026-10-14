// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{decimal128_array_from_iter, string_array_from_display_iter};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{CustomerGenerator, CustomerGeneratorIterator};

/// Generate [`Customer`]s in [`RecordBatch`] format
///
/// [`Customer`]: tpchgen::generators::Customer
///
pub struct CustomerArrow {
    inner: CustomerGeneratorIterator<'static>,
    batch_size: usize,
}

impl CustomerArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&CUSTOMER_SCHEMA)
    }

    pub fn new(generator: CustomerGenerator<'static>) -> Self {
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

impl RecordBatchIterator for CustomerArrow {
    fn schema(&self) -> &SchemaRef {
        &CUSTOMER_SCHEMA
    }
}

impl Iterator for CustomerArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let c_custkey = Int64Array::from_iter_values(rows.iter().map(|r| r.c_custkey));
        let c_name = string_array_from_display_iter(rows.iter().map(|r| r.c_name));
        let c_address = string_array_from_display_iter(rows.iter().map(|r| &r.c_address));
        let c_nationkey = Int64Array::from_iter_values(rows.iter().map(|r| r.c_nationkey));
        let c_phone = string_array_from_display_iter(rows.iter().map(|r| &r.c_phone));
        let c_acctbal = decimal128_array_from_iter(rows.iter().map(|r| r.c_acctbal));
        let c_mktsegment = string_array_from_display_iter(rows.iter().map(|r| r.c_mktsegment));
        let c_comment = string_array_from_display_iter(rows.iter().map(|r| r.c_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(c_custkey),
                Arc::new(c_name),
                Arc::new(c_address),
                Arc::new(c_nationkey),
                Arc::new(c_phone),
                Arc::new(c_acctbal),
                Arc::new(c_mktsegment),
                Arc::new(c_comment),
            ],
        )
        .map_err(Into::into);
        Some(batch)
    }
}

/// Schema for the Customer
static CUSTOMER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_customer_schema);
fn make_customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]))
}
