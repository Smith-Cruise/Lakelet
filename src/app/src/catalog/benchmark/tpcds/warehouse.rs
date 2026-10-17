// Adapted from tpcdsgen-arrow 0.1.0-alpha.1 (Apache-2.0).
// Source: https://crates.io/crates/tpcdsgen-arrow/0.1.0-alpha.1
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{address_columns, opt, sk_opt, string_array_from_opt_iter};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator, RowIter};
use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpcdsgen::config::{Session, Table};
use tpcdsgen::row::{GeneratedRow, WarehouseRowGenerator};

pub struct WarehouseArrow {
    inner: RowIter<WarehouseRowGenerator>,
    batch_size: usize,
}

impl WarehouseArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&SCHEMA)
    }

    pub fn new(session: Session) -> Self {
        let row_count = session.get_scaling().get_row_count(Table::Warehouse);
        Self {
            inner: RowIter::new(WarehouseRowGenerator::new(), session, row_count),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for WarehouseArrow {
    fn schema(&self) -> &SchemaRef {
        &SCHEMA
    }
}

impl Iterator for WarehouseArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows: Vec<_> = self
            .inner
            .by_ref()
            .map(|g| match g {
                GeneratedRow::Warehouse(r) => r,
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

        let mut w_sk: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut w_id: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut w_name: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut w_sq_ft: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut addr_rows: Vec<(tpcdsgen::types::Address, i64, u32)> =
            Vec::with_capacity(rows.len());

        for r in &rows {
            let nbm = r.null_bit_map();
            w_sk.push(sk_opt(nbm, 0, r.get_w_warehouse_sk()));
            w_id.push(opt(nbm, 1, r.get_w_warehouse_id().to_owned()));
            w_name.push(opt(nbm, 2, r.get_w_warehouse_name().to_owned()));
            w_sq_ft.push(opt(nbm, 3, r.get_w_warehouse_sq_ft()));
            addr_rows.push((r.get_w_address().clone(), nbm, 4));
        }

        let (
            street_number,
            street_name,
            street_type,
            suite_number,
            city,
            county,
            state,
            zip,
            country,
            gmt_offset,
        ) = address_columns(addr_rows.iter().map(|(a, nbm, base)| (a, *nbm, *base)));

        Some(
            RecordBatch::try_new(
                Arc::clone(self.schema()),
                vec![
                    Arc::new(Int64Array::from(w_sk)),
                    Arc::new(string_array_from_opt_iter(
                        w_id.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(string_array_from_opt_iter(
                        w_name.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(Int32Array::from(w_sq_ft)),
                    Arc::new(street_number),
                    Arc::new(street_name),
                    Arc::new(street_type),
                    Arc::new(suite_number),
                    Arc::new(city),
                    Arc::new(county),
                    Arc::new(state),
                    Arc::new(zip),
                    Arc::new(country),
                    Arc::new(gmt_offset),
                ],
            )
            .map_err(Into::into),
        )
    }
}

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_schema);

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("w_warehouse_sk", DataType::Int64, true),
        Field::new("w_warehouse_id", DataType::Utf8, true),
        Field::new("w_warehouse_name", DataType::Utf8, true),
        Field::new("w_warehouse_sq_ft", DataType::Int32, true),
        Field::new("w_street_number", DataType::Int32, true),
        Field::new("w_street_name", DataType::Utf8, true),
        Field::new("w_street_type", DataType::Utf8, true),
        Field::new("w_suite_number", DataType::Utf8, true),
        Field::new("w_city", DataType::Utf8, true),
        Field::new("w_county", DataType::Utf8, true),
        Field::new("w_state", DataType::Utf8, true),
        Field::new("w_zip", DataType::Utf8, true),
        Field::new("w_country", DataType::Utf8, true),
        Field::new("w_gmt_offset", DataType::Int32, true),
    ]))
}
