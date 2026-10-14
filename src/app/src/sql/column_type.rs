use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_expr::sqlparser::ast::Ident;

/// Format SQL column types without exposing Arrow storage layouts.
pub(crate) fn sql_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "NULL".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt8 => "TINYINT UNSIGNED".into(),
        DataType::UInt16 => "SMALLINT UNSIGNED".into(),
        DataType::UInt32 => "INT UNSIGNED".into(),
        DataType::UInt64 => "BIGINT UNSIGNED".into(),
        DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BINARY".into(),
        DataType::FixedSizeBinary(length) => format!("BINARY({length})"),
        DataType::Date32 | DataType::Date64 => "DATE".into(),
        DataType::Timestamp(unit, timezone) => {
            let suffix = if timezone.is_some() {
                " WITH TIME ZONE"
            } else {
                ""
            };
            format!("TIMESTAMP({}){suffix}", time_precision(unit))
        }
        DataType::Time32(unit) | DataType::Time64(unit) => {
            format!("TIME({})", time_precision(unit))
        }
        DataType::Interval(_) => "INTERVAL".into(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("DECIMAL({precision}, {scale})"),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", sql_type_name(field.data_type()))
        }
        DataType::Map(entries, _) => {
            if let DataType::Struct(fields) = entries.data_type()
                && fields.len() == 2
            {
                return format!(
                    "MAP<{}, {}>",
                    sql_type_name(fields[0].data_type()),
                    sql_type_name(fields[1].data_type())
                );
            }
            data_type.to_string()
        }
        DataType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|field| format!(
                    "{} {}",
                    Ident::with_quote('`', field.name()),
                    sql_type_name(field.data_type())
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        DataType::Union(fields, _) => format!(
            "UNIONTYPE<{}>",
            fields
                .iter()
                .map(|(_, field)| sql_type_name(field.data_type()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        DataType::Dictionary(_, value) => sql_type_name(value),
        DataType::RunEndEncoded(_, value) => sql_type_name(value.data_type()),
        // Preserve types without a SQL equivalent rather than mislabeling them.
        _ => data_type.to_string(),
    }
}

fn time_precision(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}
