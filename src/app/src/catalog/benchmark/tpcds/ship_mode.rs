// Adapted from tpcdsgen-arrow 0.1.0-alpha.1 (Apache-2.0).
// Source: https://crates.io/crates/tpcdsgen-arrow/0.1.0-alpha.1
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{opt, sk_opt, string_array_from_opt_iter};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator, RowIter};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpcdsgen::config::{Session, Table};
use tpcdsgen::row::{GeneratedRow, ShipModeRowGenerator};

pub struct ShipModeArrow {
    inner: RowIter<ShipModeRowGenerator>,
    batch_size: usize,
}

impl ShipModeArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&SCHEMA)
    }

    pub fn new(session: Session) -> Self {
        let row_count = session.get_scaling().get_row_count(Table::ShipMode);
        Self {
            inner: RowIter::new(ShipModeRowGenerator::new(), session, row_count),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for ShipModeArrow {
    fn schema(&self) -> &SchemaRef {
        &SCHEMA
    }
}

impl Iterator for ShipModeArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows: Vec<_> = self
            .inner
            .by_ref()
            .map(|g| match g {
                GeneratedRow::ShipMode(r) => r,
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

        let mut sm_sk: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut sm_id: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut sm_type: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut sm_code: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut sm_carrier: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut sm_contract: Vec<Option<String>> = Vec::with_capacity(rows.len());

        for r in &rows {
            let nbm = r.null_bit_map();
            sm_sk.push(sk_opt(nbm, 0, r.get_sm_ship_mode_sk()));
            sm_id.push(opt(nbm, 1, r.get_sm_ship_mode_id().to_owned()));
            sm_type.push(opt(nbm, 2, r.get_sm_type().to_owned()));
            sm_code.push(opt(nbm, 3, r.get_sm_code().to_owned()));
            sm_carrier.push(opt(nbm, 4, r.get_sm_carrier().to_owned()));
            sm_contract.push(opt(nbm, 5, r.get_sm_contract().to_owned()));
        }

        Some(
            RecordBatch::try_new(
                Arc::clone(self.schema()),
                vec![
                    Arc::new(Int64Array::from(sm_sk)),
                    Arc::new(string_array_from_opt_iter(
                        sm_id.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(string_array_from_opt_iter(
                        sm_type.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(string_array_from_opt_iter(
                        sm_code.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(string_array_from_opt_iter(
                        sm_carrier.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(string_array_from_opt_iter(
                        sm_contract.iter().map(|s| s.as_deref()),
                    )),
                ],
            )
            .map_err(Into::into),
        )
    }
}

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_schema);

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sm_ship_mode_sk", DataType::Int64, true),
        Field::new("sm_ship_mode_id", DataType::Utf8, true),
        Field::new("sm_type", DataType::Utf8, true),
        Field::new("sm_code", DataType::Utf8, true),
        Field::new("sm_carrier", DataType::Utf8, true),
        Field::new("sm_contract", DataType::Utf8, true),
    ]))
}
