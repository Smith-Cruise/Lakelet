use super::{BatchIterator, GenerationOptions, Table};
use datafusion::arrow::datatypes::SchemaRef;
use std::sync::{Arc, LazyLock};
use tpchgen_arrow::RecordBatchIterator;

macro_rules! table {
    ($name:literal, $adapter:ident, $generator:ident) => {
        Table {
            name: $name,
            schema: || {
                static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
                    let generator = tpchgen::generators::$generator::new(1.0, 1, 1);
                    Arc::clone(tpchgen_arrow::$adapter::new(generator).schema())
                });
                Arc::clone(&SCHEMA)
            },
            generate: |options: GenerationOptions| {
                let generator = tpchgen::generators::$generator::new(
                    options.scale_factor,
                    options.partition + 1,
                    options.partitions,
                );
                let batches =
                    tpchgen_arrow::$adapter::new(generator).with_batch_size(options.batch_size);
                Ok(Box::new(batches.map(Ok)) as BatchIterator)
            },
        }
    };
}

pub(super) const TABLES: &[Table] = &[
    table!("customer", CustomerArrow, CustomerGenerator),
    table!("lineitem", LineItemArrow, LineItemGenerator),
    table!("nation", NationArrow, NationGenerator),
    table!("orders", OrderArrow, OrderGenerator),
    table!("part", PartArrow, PartGenerator),
    table!("partsupp", PartSuppArrow, PartSuppGenerator),
    table!("region", RegionArrow, RegionGenerator),
    table!("supplier", SupplierArrow, SupplierGenerator),
];
