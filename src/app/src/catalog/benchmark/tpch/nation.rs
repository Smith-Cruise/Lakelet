// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{NationGenerator, NationGeneratorIterator};

/// Generate  [`Nation`]s in [`RecordBatch`] format
///
/// [`Nation`]: tpchgen::generators::Nation
///
pub struct NationArrow {
    inner: NationGeneratorIterator<'static>,
    batch_size: usize,
}

impl NationArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&NATION_SCHEMA)
    }

    pub fn new(generator: NationGenerator<'static>) -> Self {
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

impl RecordBatchIterator for NationArrow {
    fn schema(&self) -> &SchemaRef {
        &NATION_SCHEMA
    }
}

impl Iterator for NationArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let n_nationkey = Int64Array::from_iter_values(rows.iter().map(|r| r.n_nationkey));
        let n_name = StringArray::from_iter_values(rows.iter().map(|r| r.n_name));
        let n_regionkey = Int64Array::from_iter_values(rows.iter().map(|r| r.n_regionkey));
        let n_comment = StringArray::from_iter_values(rows.iter().map(|r| r.n_comment));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(n_nationkey),
                Arc::new(n_name),
                Arc::new(n_regionkey),
                Arc::new(n_comment),
            ],
        )
        .map_err(Into::into);
        Some(batch)
    }
}

/// Schema for the Nation
static NATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_nation_schema);
fn make_nation_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, false),
    ]))
}
