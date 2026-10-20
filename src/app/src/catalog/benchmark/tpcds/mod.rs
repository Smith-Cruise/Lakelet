use super::{BatchIterator, GenerationOptions, Table};
use tpcdsgen::config::SessionBuilder;

macro_rules! table {
    ($name:literal, $adapter:ident) => {
        Table {
            name: $name,
            schema: tpcdsgen_arrow::$adapter::schema_ref,
            generate: |options: GenerationOptions| {
                let session = SessionBuilder::new()
                    .with_scale_factor(options.scale_factor)
                    .build()
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
                let batches =
                    tpcdsgen_arrow::$adapter::new(session).with_batch_size(options.batch_size);
                Ok(Box::new(batches.map(|batch| batch.map_err(Into::into))) as BatchIterator)
            },
        }
    };
}

pub(super) const TABLES: &[Table] = &[
    table!("call_center", CallCenterArrow),
    table!("catalog_page", CatalogPageArrow),
    table!("catalog_returns", CatalogReturnsArrow),
    table!("catalog_sales", CatalogSalesArrow),
    table!("customer", CustomerArrow),
    table!("customer_address", CustomerAddressArrow),
    table!("customer_demographics", CustomerDemographicsArrow),
    table!("date_dim", DateDimArrow),
    table!("household_demographics", HouseholdDemographicsArrow),
    table!("income_band", IncomeBandArrow),
    table!("inventory", InventoryArrow),
    table!("item", ItemArrow),
    table!("promotion", PromotionArrow),
    table!("reason", ReasonArrow),
    table!("ship_mode", ShipModeArrow),
    table!("store", StoreArrow),
    table!("store_returns", StoreReturnsArrow),
    table!("store_sales", StoreSalesArrow),
    table!("time_dim", TimeDimArrow),
    table!("warehouse", WarehouseArrow),
    table!("web_page", WebPageArrow),
    table!("web_returns", WebReturnsArrow),
    table!("web_sales", WebSalesArrow),
    table!("web_site", WebSiteArrow),
];
