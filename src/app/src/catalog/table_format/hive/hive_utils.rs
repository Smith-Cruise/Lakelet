use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use std::collections::HashMap;

pub(crate) fn parse_statistics_from_table_properties(
    table_statistics: &mut Statistics,
    table_properties: &HashMap<String, String>,
) {
    if let Some(num_rows) = table_properties
        .get("numRows")
        .and_then(|num_rows| num_rows.trim().parse::<usize>().ok())
    {
        table_statistics.num_rows = Precision::Inexact(num_rows);
    }
    if let Some(total_byte_size) = table_properties
        .get("totalSize")
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        table_statistics.total_byte_size = Precision::Inexact(total_byte_size);
    }
}
