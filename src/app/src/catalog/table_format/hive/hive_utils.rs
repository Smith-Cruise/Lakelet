use std::collections::HashMap;

pub(crate) fn parse_hive_num_rows(table_properties: &HashMap<String, String>) -> Option<usize> {
    table_properties
        .get("numRows")
        .and_then(|num_rows| num_rows.trim().parse::<usize>().ok())
}

pub(crate) fn parse_hive_total_byte_size(table_properties: &HashMap<String, String>) -> Option<usize> {
    table_properties
        .get("totalSize")
        .and_then(|value| value.trim().parse::<usize>().ok())
}