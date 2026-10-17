// Adapted from tpchgen-arrow 3.0.0 (Apache-2.0).
// Source: https://crates.io/crates/tpchgen-arrow/3.0.0
// Local changes: Arrow 58, Utf8 strings, static schemas and fallible batches.
// See LICENSE in this directory for the upstream license.

use super::{BatchIterator, GenerationOptions, Table};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;

mod conversions;
mod customer;
mod lineitem;
mod nation;
mod order;
mod part;
mod partsupp;
mod region;
mod supplier;

const DEFAULT_BATCH_SIZE: usize = 8192;

trait RecordBatchIterator:
    Iterator<Item = Result<datafusion::arrow::record_batch::RecordBatch>> + Send
{
    fn schema(&self) -> &SchemaRef;
}

macro_rules! table {
    ($name:literal, $module:ident, $adapter:ident, $generator:ident) => {
        Table {
            name: $name,
            schema: $module::$adapter::schema_ref,
            generate: |options: GenerationOptions| {
                let generator = tpchgen::generators::$generator::new(
                    options.scale_factor,
                    options.partition + 1,
                    options.partitions,
                );
                Ok(
                    Box::new($module::$adapter::new(generator).with_batch_size(options.batch_size))
                        as BatchIterator,
                )
            },
        }
    };
}

pub(super) const TABLES: &[Table] = &[
    table!("customer", customer, CustomerArrow, CustomerGenerator),
    table!("lineitem", lineitem, LineItemArrow, LineItemGenerator),
    table!("nation", nation, NationArrow, NationGenerator),
    table!("orders", order, OrderArrow, OrderGenerator),
    table!("part", part, PartArrow, PartGenerator),
    table!("partsupp", partsupp, PartSuppArrow, PartSuppGenerator),
    table!("region", region, RegionArrow, RegionGenerator),
    table!("supplier", supplier, SupplierArrow, SupplierGenerator),
];
