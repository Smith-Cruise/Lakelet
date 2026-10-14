// Adapted from tpcdsgen-arrow 0.1.0-alpha.1 (Apache-2.0).
// Source: https://crates.io/crates/tpcdsgen-arrow/0.1.0-alpha.1
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{opt, sk_opt};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator, RowIter};
use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpcdsgen::config::{Session, Table};
use tpcdsgen::row::{GeneratedRow, InventoryRowGenerator};

pub struct InventoryArrow {
    inner: RowIter<InventoryRowGenerator>,
    batch_size: usize,
}

impl InventoryArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&SCHEMA)
    }

    pub fn new(session: Session) -> Self {
        let row_count = session.get_scaling().get_row_count(Table::Inventory);
        Self {
            inner: RowIter::new(InventoryRowGenerator::new(), session, row_count),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for InventoryArrow {
    fn schema(&self) -> &SchemaRef {
        &SCHEMA
    }
}

impl Iterator for InventoryArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows: Vec<_> = self
            .inner
            .by_ref()
            .map(|g| match g {
                GeneratedRow::Inventory(r) => r,
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

        let mut inv_date: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut inv_item: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut inv_warehouse: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut inv_qty: Vec<Option<i32>> = Vec::with_capacity(rows.len());

        for r in &rows {
            let nbm = r.null_bit_map();
            inv_date.push(sk_opt(nbm, 0, r.get_inv_date_sk()));
            inv_item.push(sk_opt(nbm, 1, r.get_inv_item_sk()));
            inv_warehouse.push(sk_opt(nbm, 2, r.get_inv_warehouse_sk()));
            inv_qty.push(opt(nbm, 3, r.get_inv_quantity_on_hand()));
        }

        Some(
            RecordBatch::try_new(
                Arc::clone(self.schema()),
                vec![
                    Arc::new(Int64Array::from(inv_date)),
                    Arc::new(Int64Array::from(inv_item)),
                    Arc::new(Int64Array::from(inv_warehouse)),
                    Arc::new(Int32Array::from(inv_qty)),
                ],
            )
            .map_err(Into::into),
        )
    }
}

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_schema);

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("inv_date_sk", DataType::Int64, true),
        Field::new("inv_item_sk", DataType::Int64, true),
        Field::new("inv_warehouse_sk", DataType::Int64, true),
        Field::new("inv_quantity_on_hand", DataType::Int32, true),
    ]))
}
