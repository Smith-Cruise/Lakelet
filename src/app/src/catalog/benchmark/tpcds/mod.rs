use super::{BatchIterator, GenerationOptions, Table};
use tpcdsgen::config::{SessionBuilder, Table as SourceTable};

macro_rules! table {
    ($name:literal, $adapter:ident, $source:ident) => {
        Table {
            name: $name,
            schema: tpcdsgen_arrow::$adapter::schema_ref,
            generate: |options: GenerationOptions| {
                let session = SessionBuilder::new()
                    .with_scale_factor(options.scale_factor)
                    .build()
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
                // Partition source rows, not output rows: sales expand each source
                // into line items and returns replay the corresponding sales sources.
                // Explicit ranges also split dimensions below the session chunk
                // API's small-table threshold.
                let rows = session.get_scaling().get_row_count(SourceTable::$source);
                let partitions = options.partitions as u64;
                let partition = options.partition as u64;
                let size = rows / partitions;
                let extra = rows % partitions;
                let start = 1 + partition * size + partition.min(extra);
                let count = size + u64::from(partition < extra);
                if count == 0 {
                    return Ok(Box::new(std::iter::empty()) as BatchIterator);
                }
                let batches = tpcdsgen_arrow::$adapter::new(session)
                    .with_source_row_range(start, start + count - 1)
                    .with_batch_size(options.batch_size);
                Ok(Box::new(batches.map(|batch| batch.map_err(Into::into))) as BatchIterator)
            },
        }
    };
}

pub(super) const TABLES: &[Table] = &[
    table!("call_center", CallCenterArrow, CallCenter),
    table!("catalog_page", CatalogPageArrow, CatalogPage),
    table!("catalog_returns", CatalogReturnsArrow, CatalogReturns),
    table!("catalog_sales", CatalogSalesArrow, CatalogSales),
    table!("customer", CustomerArrow, Customer),
    table!("customer_address", CustomerAddressArrow, CustomerAddress),
    table!(
        "customer_demographics",
        CustomerDemographicsArrow,
        CustomerDemographics
    ),
    table!("date_dim", DateDimArrow, DateDim),
    table!(
        "household_demographics",
        HouseholdDemographicsArrow,
        HouseholdDemographics
    ),
    table!("income_band", IncomeBandArrow, IncomeBand),
    table!("inventory", InventoryArrow, Inventory),
    table!("item", ItemArrow, Item),
    table!("promotion", PromotionArrow, Promotion),
    table!("reason", ReasonArrow, Reason),
    table!("ship_mode", ShipModeArrow, ShipMode),
    table!("store", StoreArrow, Store),
    table!("store_returns", StoreReturnsArrow, StoreReturns),
    table!("store_sales", StoreSalesArrow, StoreSales),
    table!("time_dim", TimeDimArrow, TimeDim),
    table!("warehouse", WarehouseArrow, Warehouse),
    table!("web_page", WebPageArrow, WebPage),
    table!("web_returns", WebReturnsArrow, WebReturns),
    table!("web_sales", WebSalesArrow, WebSales),
    table!("web_site", WebSiteArrow, WebSite),
];
