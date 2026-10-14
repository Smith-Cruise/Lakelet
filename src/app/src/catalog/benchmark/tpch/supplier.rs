// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{decimal128_array_from_iter, string_array_from_display_iter};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{SupplierGenerator, SupplierGeneratorIterator};

/// Generate [`Supplier`]s in [`RecordBatch`] format
///
/// [`Supplier`]: tpchgen::generators::Supplier
///
/// # Example:
pub struct SupplierArrow {
    inner: SupplierGeneratorIterator<'static>,
    batch_size: usize,
}

impl SupplierArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&SUPPLIER_SCHEMA)
    }

    pub fn new(generator: SupplierGenerator<'static>) -> Self {
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

impl RecordBatchIterator for SupplierArrow {
    fn schema(&self) -> &SchemaRef {
        &SUPPLIER_SCHEMA
    }
}

impl Iterator for SupplierArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let s_suppkey = Int64Array::from_iter_values(rows.iter().map(|r| r.s_suppkey));
        let s_name = string_array_from_display_iter(rows.iter().map(|r| r.s_name));
        let s_address = string_array_from_display_iter(rows.iter().map(|r| &r.s_address));
        let s_nationkey = Int64Array::from_iter_values(rows.iter().map(|r| r.s_nationkey));
        let s_phone = string_array_from_display_iter(rows.iter().map(|r| &r.s_phone));
        let s_acctbal = decimal128_array_from_iter(rows.iter().map(|r| r.s_acctbal));
        let s_comment = string_array_from_display_iter(rows.iter().map(|r| &r.s_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(s_suppkey),
                Arc::new(s_name),
                Arc::new(s_address),
                Arc::new(s_nationkey),
                Arc::new(s_phone),
                Arc::new(s_acctbal),
                Arc::new(s_comment),
            ],
        )
        .map_err(Into::into);
        Some(batch)
    }
}

/// Schema for the PartSupp
static SUPPLIER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_supplier_schema);
fn make_supplier_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("s_comment", DataType::Utf8, false),
    ]))
}
