use super::{BatchIterator, GenerationOptions, Table};

macro_rules! table {
    ($name:literal, $adapter:ident, $generator:ident) => {
        Table {
            name: $name,
            schema: tpchgen_arrow::$adapter::schema_ref,
            generate: |options: GenerationOptions| {
                let generator = tpchgen::generators::$generator::new(
                    options.scale_factor,
                    options.partition + 1,
                    options.partitions,
                );
                let batches =
                    tpchgen_arrow::$adapter::new(generator).with_batch_size(options.batch_size);
                Ok(Box::new(batches.map(|batch| batch.map_err(Into::into))) as BatchIterator)
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
