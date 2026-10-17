// Adapted from tpcdsgen-arrow 0.1.0-alpha.1 (Apache-2.0).
// Source: https://crates.io/crates/tpcdsgen-arrow/0.1.0-alpha.1
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::opt;
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator, RowIter};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpcdsgen::config::{Session, Table};
use tpcdsgen::row::{GeneratedRow, IncomeBandRowGenerator};

pub struct IncomeBandArrow {
    inner: RowIter<IncomeBandRowGenerator>,
    batch_size: usize,
}

impl IncomeBandArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&SCHEMA)
    }

    pub fn new(session: Session) -> Self {
        let row_count = session.get_scaling().get_row_count(Table::IncomeBand);
        Self {
            inner: RowIter::new(IncomeBandRowGenerator::new(), session, row_count),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for IncomeBandArrow {
    fn schema(&self) -> &SchemaRef {
        &SCHEMA
    }
}

impl Iterator for IncomeBandArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows: Vec<_> = self
            .inner
            .by_ref()
            .map(|g| match g {
                GeneratedRow::IncomeBand(r) => r,
                _ => unreachable!(),
            })
            .take(self.batch_size)
            .collect();
        if let Some(error) = self.inner.take_error() {
            return Some(Err(error));
        }
        if rows.is_empty() {
            return None;
        }

        let mut band_id: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut lower: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut upper: Vec<Option<i32>> = Vec::with_capacity(rows.len());

        for r in &rows {
            let nbm = r.null_bit_map();
            band_id.push(opt(nbm, 0, r.get_ib_income_band_id()));
            lower.push(opt(nbm, 1, r.get_ib_lower_bound()));
            upper.push(opt(nbm, 2, r.get_ib_upper_bound()));
        }

        Some(
            RecordBatch::try_new(
                Arc::clone(self.schema()),
                vec![
                    Arc::new(Int32Array::from(band_id)),
                    Arc::new(Int32Array::from(lower)),
                    Arc::new(Int32Array::from(upper)),
                ],
            )
            .map_err(Into::into),
        )
    }
}

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_schema);

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ib_income_band_id", DataType::Int32, false),
        Field::new("ib_lower_bound", DataType::Int32, false),
        Field::new("ib_upper_bound", DataType::Int32, false),
    ]))
}
