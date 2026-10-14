// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{decimal128_array_from_iter, string_array_from_display_iter};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{PartGenerator, PartGeneratorIterator};

/// Generate [`Part`]s in [`RecordBatch`] format
///
/// [`Part`]: tpchgen::generators::Part
///
pub struct PartArrow {
    inner: PartGeneratorIterator<'static>,
    batch_size: usize,
}

impl PartArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&PART_SCHEMA)
    }

    pub fn new(generator: PartGenerator<'static>) -> Self {
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

impl RecordBatchIterator for PartArrow {
    fn schema(&self) -> &SchemaRef {
        &PART_SCHEMA
    }
}

impl Iterator for PartArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let p_partkey = Int64Array::from_iter_values(rows.iter().map(|r| r.p_partkey));
        let p_name = string_array_from_display_iter(rows.iter().map(|r| &r.p_name));
        let p_mfgr = string_array_from_display_iter(rows.iter().map(|r| r.p_mfgr));
        let p_brand = string_array_from_display_iter(rows.iter().map(|r| r.p_brand));
        let p_type = StringArray::from_iter_values(rows.iter().map(|r| r.p_type));
        let p_size = Int32Array::from_iter_values(rows.iter().map(|r| r.p_size));
        let p_container = StringArray::from_iter_values(rows.iter().map(|r| r.p_container));
        let p_retailprice = decimal128_array_from_iter(rows.iter().map(|r| r.p_retailprice));
        let p_comment = StringArray::from_iter_values(rows.iter().map(|r| r.p_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(p_partkey),
                Arc::new(p_name),
                Arc::new(p_mfgr),
                Arc::new(p_brand),
                Arc::new(p_type),
                Arc::new(p_size),
                Arc::new(p_container),
                Arc::new(p_retailprice),
                Arc::new(p_comment),
            ],
        )
        .map_err(Into::into);
        Some(batch)
    }
}

/// Schema for the Part
static PART_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_part_schema);
fn make_part_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
        Field::new("p_comment", DataType::Utf8, false),
    ]))
}
