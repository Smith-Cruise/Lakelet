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
        .get("rawDataSize")
        .or_else(|| table_properties.get("totalSize"))
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        table_statistics.total_byte_size = Precision::Inexact(total_byte_size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;

    #[test]
    fn raw_data_size_takes_precedence_over_total_size() {
        let mut statistics = Statistics::new_unknown(&Schema::empty());
        let properties = HashMap::from([
            ("rawDataSize".to_string(), "80".to_string()),
            ("totalSize".to_string(), "40".to_string()),
        ]);

        parse_statistics_from_table_properties(&mut statistics, &properties);

        assert_eq!(statistics.total_byte_size, Precision::Inexact(80));

        let mut statistics = Statistics::new_unknown(&Schema::empty());
        let properties = HashMap::from([("totalSize".to_string(), "40".to_string())]);

        parse_statistics_from_table_properties(&mut statistics, &properties);

        assert_eq!(statistics.total_byte_size, Precision::Inexact(40));
    }
}
