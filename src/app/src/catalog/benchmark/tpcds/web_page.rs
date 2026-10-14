// Adapted from tpcdsgen-arrow 0.1.0-alpha.1 (Apache-2.0).
// Source: https://crates.io/crates/tpcdsgen-arrow/0.1.0-alpha.1
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::conversions::{bool_to_yn, julian_to_date32, opt, sk_opt, string_array_from_opt_iter};
use super::{DEFAULT_BATCH_SIZE, RecordBatchIterator, RowIter};
use datafusion::arrow::array::{Date32Array, Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpcdsgen::config::{Session, Table};
use tpcdsgen::row::{GeneratedRow, WebPageRowGenerator};

pub struct WebPageArrow {
    inner: RowIter<WebPageRowGenerator>,
    batch_size: usize,
}

impl WebPageArrow {
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&SCHEMA)
    }

    pub fn new(session: Session) -> Self {
        let row_count = session.get_scaling().get_row_count(Table::WebPage);
        Self {
            inner: RowIter::new(WebPageRowGenerator::new(), session, row_count),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for WebPageArrow {
    fn schema(&self) -> &SchemaRef {
        &SCHEMA
    }
}

impl Iterator for WebPageArrow {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows: Vec<_> = self
            .inner
            .by_ref()
            .map(|g| match g {
                GeneratedRow::WebPage(r) => r,
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

        let mut wp_sk: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut wp_id: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut wp_rec_start: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut wp_rec_end: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut wp_creation_date: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut wp_access_date: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut wp_autogen: Vec<Option<&'static str>> = Vec::with_capacity(rows.len());
        let mut wp_customer: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut wp_url: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut wp_type: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut wp_char_count: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut wp_link_count: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut wp_image_count: Vec<Option<i32>> = Vec::with_capacity(rows.len());
        let mut wp_max_ad_count: Vec<Option<i32>> = Vec::with_capacity(rows.len());

        for r in &rows {
            let nbm = r.null_bit_map();
            wp_sk.push(sk_opt(nbm, 0, r.get_wp_page_sk()));
            wp_id.push(opt(nbm, 1, r.get_wp_page_id().to_owned()));
            wp_rec_start.push(julian_to_date32(r.get_wp_rec_start_date_id()));
            wp_rec_end.push(julian_to_date32(r.get_wp_rec_end_date_id()));
            wp_creation_date.push(sk_opt(nbm, 4, r.get_wp_creation_date_sk()));
            wp_access_date.push(sk_opt(nbm, 5, r.get_wp_access_date_sk()));
            wp_autogen.push(opt(nbm, 6, bool_to_yn(r.get_wp_autogen_flag())));
            wp_customer.push(sk_opt(nbm, 7, r.get_wp_customer_sk()));
            wp_url.push(opt(nbm, 8, r.get_wp_url().to_owned()));
            wp_type.push(opt(nbm, 9, r.get_wp_type().to_owned()));
            wp_char_count.push(opt(nbm, 10, r.get_wp_char_count()));
            wp_link_count.push(opt(nbm, 11, r.get_wp_link_count()));
            wp_image_count.push(opt(nbm, 12, r.get_wp_image_count()));
            wp_max_ad_count.push(opt(nbm, 13, r.get_wp_max_ad_count()));
        }

        Some(
            RecordBatch::try_new(
                Arc::clone(self.schema()),
                vec![
                    Arc::new(Int64Array::from(wp_sk)),
                    Arc::new(string_array_from_opt_iter(
                        wp_id.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(Date32Array::from(wp_rec_start)),
                    Arc::new(Date32Array::from(wp_rec_end)),
                    Arc::new(Int64Array::from(wp_creation_date)),
                    Arc::new(Int64Array::from(wp_access_date)),
                    Arc::new(string_array_from_opt_iter(wp_autogen.iter().copied())),
                    Arc::new(Int64Array::from(wp_customer)),
                    Arc::new(string_array_from_opt_iter(
                        wp_url.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(string_array_from_opt_iter(
                        wp_type.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(Int32Array::from(wp_char_count)),
                    Arc::new(Int32Array::from(wp_link_count)),
                    Arc::new(Int32Array::from(wp_image_count)),
                    Arc::new(Int32Array::from(wp_max_ad_count)),
                ],
            )
            .map_err(Into::into),
        )
    }
}

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_schema);

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("wp_web_page_sk", DataType::Int64, true),
        Field::new("wp_web_page_id", DataType::Utf8, true),
        Field::new("wp_rec_start_date", DataType::Date32, true),
        Field::new("wp_rec_end_date", DataType::Date32, true),
        Field::new("wp_creation_date_sk", DataType::Int64, true),
        Field::new("wp_access_date_sk", DataType::Int64, true),
        Field::new("wp_autogen_flag", DataType::Utf8, true),
        Field::new("wp_customer_sk", DataType::Int64, true),
        Field::new("wp_url", DataType::Utf8, true),
        Field::new("wp_type", DataType::Utf8, true),
        Field::new("wp_char_count", DataType::Int32, true),
        Field::new("wp_link_count", DataType::Int32, true),
        Field::new("wp_image_count", DataType::Int32, true),
        Field::new("wp_max_ad_count", DataType::Int32, true),
    ]))
}
