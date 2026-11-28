pub(super) mod rest;
mod schema;

use datafusion::common::DataFusionError;

fn to_datafusion_error(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
