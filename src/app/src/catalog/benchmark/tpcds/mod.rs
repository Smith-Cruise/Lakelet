// Adapted from tpcdsgen-arrow 0.1.0-alpha.1 (Apache-2.0).
// Source: https://crates.io/crates/tpcdsgen-arrow/0.1.0-alpha.1
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::{BatchIterator, GenerationOptions, Table};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;

mod call_center;
mod catalog_page;
mod catalog_returns;
mod catalog_sales;
mod conversions;
mod customer;
mod customer_address;
mod customer_demographics;
mod date_dim;
mod household_demographics;
mod income_band;
mod inventory;
mod item;
mod promotion;
mod reason;
mod ship_mode;
mod store;
mod store_returns;
mod store_sales;
mod time_dim;
mod warehouse;
mod web_page;
mod web_returns;
mod web_sales;
mod web_site;

const DEFAULT_BATCH_SIZE: usize = 8192;

trait RecordBatchIterator:
    Iterator<Item = Result<datafusion::arrow::record_batch::RecordBatch>> + Send
{
    fn schema(&self) -> &SchemaRef;
}

macro_rules! table {
    ($name:literal, $module:ident, $adapter:ident) => {
        Table {
            name: $name,
            schema: $module::$adapter::schema_ref,
            generate: |options: GenerationOptions| {
                let session = tpcdsgen::config::SessionBuilder::new()
                    .with_scale_factor(options.scale_factor)
                    .with_compat_mode(tpcdsgen::config::CompatMode::Trino)
                    .build()
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
                Ok(
                    Box::new($module::$adapter::new(session).with_batch_size(options.batch_size))
                        as BatchIterator,
                )
            },
        }
    };
}

pub(super) const TABLES: &[Table] = &[
    table!("call_center", call_center, CallCenterArrow),
    table!("catalog_page", catalog_page, CatalogPageArrow),
    table!("catalog_returns", catalog_returns, CatalogReturnsArrow),
    table!("catalog_sales", catalog_sales, CatalogSalesArrow),
    table!("customer", customer, CustomerArrow),
    table!("customer_address", customer_address, CustomerAddressArrow),
    table!(
        "customer_demographics",
        customer_demographics,
        CustomerDemographicsArrow
    ),
    table!("date_dim", date_dim, DateDimArrow),
    table!(
        "household_demographics",
        household_demographics,
        HouseholdDemographicsArrow
    ),
    table!("income_band", income_band, IncomeBandArrow),
    table!("inventory", inventory, InventoryArrow),
    table!("item", item, ItemArrow),
    table!("promotion", promotion, PromotionArrow),
    table!("reason", reason, ReasonArrow),
    table!("ship_mode", ship_mode, ShipModeArrow),
    table!("store", store, StoreArrow),
    table!("store_returns", store_returns, StoreReturnsArrow),
    table!("store_sales", store_sales, StoreSalesArrow),
    table!("time_dim", time_dim, TimeDimArrow),
    table!("warehouse", warehouse, WarehouseArrow),
    table!("web_page", web_page, WebPageArrow),
    table!("web_returns", web_returns, WebReturnsArrow),
    table!("web_sales", web_sales, WebSalesArrow),
    table!("web_site", web_site, WebSiteArrow),
];

use std::collections::VecDeque;
use tpcdsgen::config::Session;
use tpcdsgen::row::{GeneratedRow, RowGenerator};

/// Adapts a [`RowGenerator`] into a streaming [`Iterator`] of [`GeneratedRow`]s.
///
/// Handles both simple generators (one row per call, `should_end_row` always
/// true) and paired fact-table generators (multiple calls per source row,
/// `should_end_row` signals when to advance the row counter).
pub(crate) struct RowIter<G: RowGenerator> {
    generator: G,
    session: Session,
    current_row: i64,
    row_count: i64,
    pending: VecDeque<GeneratedRow>,
    error: Option<datafusion::common::DataFusionError>,
}

impl<G: RowGenerator> RowIter<G> {
    fn take_error(&mut self) -> Option<datafusion::common::DataFusionError> {
        self.error.take()
    }

    pub(crate) fn new(generator: G, session: Session, row_count: i64) -> Self {
        Self {
            generator,
            session,
            current_row: 1,
            row_count,
            pending: VecDeque::new(),
            error: None,
        }
    }
}

impl<G: RowGenerator> Iterator for RowIter<G> {
    type Item = GeneratedRow;

    fn next(&mut self) -> Option<GeneratedRow> {
        while self.pending.is_empty() {
            if self.current_row > self.row_count {
                return None;
            }
            let result = match self.generator.generate_row_and_child_rows(
                self.current_row,
                &self.session,
                None,
                None,
            ) {
                Ok(result) => result,
                Err(error) => {
                    self.current_row = self.row_count + 1;
                    self.error = Some(datafusion::common::DataFusionError::External(Box::new(
                        error,
                    )));
                    return None;
                }
            };
            for row in result.get_rows() {
                self.pending.push_back(row.clone());
            }
            if result.should_end_row() {
                self.generator.consume_remaining_seeds_for_row();
                self.current_row += 1;
            }
        }
        self.pending.pop_front()
    }
}
