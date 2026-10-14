// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{RegionGenerator, RegionGeneratorIterator};

/// Generate  [`Region`]s in [`RecordBatch`] format
///
/// [`Region`]: tpchgen::generators::Region
///
pub struct RegionArrow {
    inner: RegionGeneratorIterator<'static>,
    batch_size: usize,
}

impl RegionArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&REGION_SCHEMA)
    }

    pub fn new(generator: RegionGenerator<'static>) -> Self {
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

impl RecordBatchIterator for RegionArrow {
    fn schema(&self) -> &SchemaRef {
        &REGION_SCHEMA
    }
}

impl Iterator for RegionArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let r_regionkey = Int64Array::from_iter_values(rows.iter().map(|r| r.r_regionkey));
        let r_name = StringArray::from_iter_values(rows.iter().map(|r| r.r_name));
        let r_comment = StringArray::from_iter_values(rows.iter().map(|r| r.r_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![Arc::new(r_regionkey), Arc::new(r_name), Arc::new(r_comment)],
        )
        .map_err(Into::into);
        Some(batch)
    }
}

/// Schema for the Region
static REGION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_region_schema);
fn make_region_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, false),
    ]))
}
