use aws_sdk_glue::Client;
use aws_sdk_glue::types::{
    ColumnStatistics as GlueColumnStatistics, ColumnStatisticsType, DecimalNumber,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::common::stats::{ColumnStatistics, Precision};
use datafusion::datasource::table_schema::TableSchema;
use futures::{StreamExt, stream};
use std::collections::HashMap;

const MAX_COLUMNS_PER_REQUEST: usize = 100;
const MAX_CONCURRENT_REQUESTS: usize = 4;

/// Statistics are best-effort: failed requests and columns without statistics
/// (e.g. tables that were never analyzed) silently stay unknown.
pub async fn load_glue_columns_statistics(
    client: &Client,
    schema_name: &str,
    table_name: &str,
    table_schema: &TableSchema,
) -> Vec<ColumnStatistics> {
    let column_names: Vec<_> = table_schema
        .file_schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    if column_names.is_empty() {
        return table_schema
            .table_schema()
            .fields()
            .iter()
            .map(|_| ColumnStatistics::new_unknown())
            .collect();
    }

    let column_batches = batch_column_names(column_names);
    let requests = column_batches.into_iter().map(|column_names| {
        let client = client.clone();
        let database_name = schema_name.to_string();
        let table_name = table_name.to_string();
        async move {
            client
                .get_column_statistics_for_table()
                .database_name(database_name)
                .table_name(table_name)
                .set_column_names(Some(column_names))
                .send()
                .await
                .ok()
        }
    });

    let responses: Vec<_> = stream::iter(requests)
        .buffer_unordered(MAX_CONCURRENT_REQUESTS)
        .collect()
        .await;

    let glue_statistics: Vec<_> = responses
        .into_iter()
        .flatten()
        .flat_map(|response| response.column_statistics_list().to_vec())
        .collect();

    convert_glue_columns_statistics(table_schema, &glue_statistics)
}

fn batch_column_names(column_names: Vec<String>) -> Vec<Vec<String>> {
    column_names
        .chunks(MAX_COLUMNS_PER_REQUEST)
        .map(<[String]>::to_vec)
        .collect()
}

fn convert_glue_columns_statistics(
    table_schema: &TableSchema,
    glue_statistics: &[GlueColumnStatistics],
) -> Vec<ColumnStatistics> {
    let statistics_by_name: HashMap<_, _> = glue_statistics
        .iter()
        .map(|statistics| (statistics.column_name(), statistics))
        .collect();
    let mut column_statistics: Vec<_> = table_schema
        .table_schema()
        .fields()
        .iter()
        .map(|_| ColumnStatistics::new_unknown())
        .collect();
    for (index, field) in table_schema.table_schema().fields().iter().enumerate() {
        if let Some(glue_statistics) = statistics_by_name.get(field.name().as_str()) {
            column_statistics[index] = convert_glue_column_statistics(field, glue_statistics);
        }
    }

    column_statistics
}

fn convert_glue_column_statistics(
    field: &Field,
    statistics: &GlueColumnStatistics,
) -> ColumnStatistics {
    let Some(data) = statistics.statistics_data() else {
        return ColumnStatistics::new_unknown();
    };

    match data.r#type() {
        ColumnStatisticsType::Boolean => data
            .boolean_column_statistics_data()
            .filter(|_| matches!(field.data_type(), DataType::Boolean))
            .map(|data| {
                let false_count = data.number_of_falses();
                let true_count = data.number_of_trues();
                if false_count < 0 || true_count < 0 {
                    return ColumnStatistics {
                        null_count: inexact_count(data.number_of_nulls()),
                        ..ColumnStatistics::new_unknown()
                    };
                }
                let distinct_count = match (false_count > 0, true_count > 0) {
                    (false, false) => 0,
                    (true, true) => 2,
                    _ => 1,
                };
                let (min_value, max_value) = match (false_count > 0, true_count > 0) {
                    (true, true) => (
                        inexact_scalar(ScalarValue::Boolean(Some(false))),
                        inexact_scalar(ScalarValue::Boolean(Some(true))),
                    ),
                    (true, false) => {
                        let value = inexact_scalar(ScalarValue::Boolean(Some(false)));
                        (value.clone(), value)
                    }
                    (false, true) => {
                        let value = inexact_scalar(ScalarValue::Boolean(Some(true)));
                        (value.clone(), value)
                    }
                    (false, false) => (Precision::Absent, Precision::Absent),
                };
                ColumnStatistics {
                    null_count: inexact_count(data.number_of_nulls()),
                    min_value,
                    max_value,
                    distinct_count: Precision::Inexact(distinct_count),
                    ..ColumnStatistics::new_unknown()
                }
            })
            .unwrap_or_else(ColumnStatistics::new_unknown),
        ColumnStatisticsType::Long => data
            .long_column_statistics_data()
            .map(|data| ColumnStatistics {
                null_count: inexact_count(data.number_of_nulls()),
                min_value: scalar_from_i64(data.minimum_value(), field.data_type())
                    .map(inexact_scalar)
                    .unwrap_or(Precision::Absent),
                max_value: scalar_from_i64(data.maximum_value(), field.data_type())
                    .map(inexact_scalar)
                    .unwrap_or(Precision::Absent),
                distinct_count: inexact_count(data.number_of_distinct_values()),
                ..ColumnStatistics::new_unknown()
            })
            .unwrap_or_else(ColumnStatistics::new_unknown),
        ColumnStatisticsType::Double => data
            .double_column_statistics_data()
            .map(|data| ColumnStatistics {
                null_count: inexact_count(data.number_of_nulls()),
                min_value: scalar_from_f64(data.minimum_value(), field.data_type())
                    .map(inexact_scalar)
                    .unwrap_or(Precision::Absent),
                max_value: scalar_from_f64(data.maximum_value(), field.data_type())
                    .map(inexact_scalar)
                    .unwrap_or(Precision::Absent),
                distinct_count: inexact_count(data.number_of_distinct_values()),
                ..ColumnStatistics::new_unknown()
            })
            .unwrap_or_else(ColumnStatistics::new_unknown),
        ColumnStatisticsType::Decimal => data
            .decimal_column_statistics_data()
            .map(|data| ColumnStatistics {
                null_count: inexact_count(data.number_of_nulls()),
                min_value: data
                    .minimum_value()
                    .and_then(|value| decimal_scalar(value, field.data_type()))
                    .map(inexact_scalar)
                    .unwrap_or(Precision::Absent),
                max_value: data
                    .maximum_value()
                    .and_then(|value| decimal_scalar(value, field.data_type()))
                    .map(inexact_scalar)
                    .unwrap_or(Precision::Absent),
                distinct_count: inexact_count(data.number_of_distinct_values()),
                ..ColumnStatistics::new_unknown()
            })
            .unwrap_or_else(ColumnStatistics::new_unknown),
        ColumnStatisticsType::Date => data
            .date_column_statistics_data()
            .map(|data| ColumnStatistics {
                null_count: inexact_count(data.number_of_nulls()),
                min_value: data
                    .minimum_value()
                    .and_then(|value| datetime_scalar(value, field.data_type()))
                    .map(inexact_scalar)
                    .unwrap_or(Precision::Absent),
                max_value: data
                    .maximum_value()
                    .and_then(|value| datetime_scalar(value, field.data_type()))
                    .map(inexact_scalar)
                    .unwrap_or(Precision::Absent),
                distinct_count: inexact_count(data.number_of_distinct_values()),
                ..ColumnStatistics::new_unknown()
            })
            .unwrap_or_else(ColumnStatistics::new_unknown),
        ColumnStatisticsType::String => data
            .string_column_statistics_data()
            .filter(|_| matches!(field.data_type(), DataType::Utf8))
            .map(|data| ColumnStatistics {
                null_count: inexact_count(data.number_of_nulls()),
                distinct_count: inexact_count(data.number_of_distinct_values()),
                ..ColumnStatistics::new_unknown()
            })
            .unwrap_or_else(ColumnStatistics::new_unknown),
        ColumnStatisticsType::Binary => data
            .binary_column_statistics_data()
            .filter(|_| matches!(field.data_type(), DataType::Binary))
            .map(|data| ColumnStatistics {
                null_count: inexact_count(data.number_of_nulls()),
                ..ColumnStatistics::new_unknown()
            })
            .unwrap_or_else(ColumnStatistics::new_unknown),
        _ => ColumnStatistics::new_unknown(),
    }
}

fn inexact_count(value: i64) -> Precision<usize> {
    usize::try_from(value)
        .map(Precision::Inexact)
        .unwrap_or(Precision::Absent)
}

fn inexact_scalar(value: ScalarValue) -> Precision<ScalarValue> {
    Precision::Inexact(value)
}

fn scalar_from_i64(value: i64, data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Int8 => i8::try_from(value)
            .ok()
            .map(|value| ScalarValue::Int8(Some(value))),
        DataType::Int16 => i16::try_from(value)
            .ok()
            .map(|value| ScalarValue::Int16(Some(value))),
        DataType::Int32 => i32::try_from(value)
            .ok()
            .map(|value| ScalarValue::Int32(Some(value))),
        DataType::Int64 => Some(ScalarValue::Int64(Some(value))),
        _ => None,
    }
}

fn scalar_from_f64(value: f64, data_type: &DataType) -> Option<ScalarValue> {
    if !value.is_finite() {
        return None;
    }
    match data_type {
        DataType::Float32 => {
            let value = value as f32;
            value
                .is_finite()
                .then_some(ScalarValue::Float32(Some(value)))
        }
        DataType::Float64 => Some(ScalarValue::Float64(Some(value))),
        _ => None,
    }
}

fn datetime_scalar(
    value: &aws_sdk_glue::primitives::DateTime,
    data_type: &DataType,
) -> Option<ScalarValue> {
    match data_type {
        DataType::Date32 => i32::try_from(value.secs().div_euclid(86_400))
            .ok()
            .map(|value| ScalarValue::Date32(Some(value))),
        DataType::Timestamp(unit, timezone) => {
            let seconds = value.secs();
            let nanos = i64::from(value.subsec_nanos());
            let timestamp = match unit {
                TimeUnit::Second => Some(seconds),
                TimeUnit::Millisecond => seconds
                    .checked_mul(1_000)
                    .and_then(|value| value.checked_add(nanos / 1_000_000)),
                TimeUnit::Microsecond => seconds
                    .checked_mul(1_000_000)
                    .and_then(|value| value.checked_add(nanos / 1_000)),
                TimeUnit::Nanosecond => seconds
                    .checked_mul(1_000_000_000)
                    .and_then(|value| value.checked_add(nanos)),
            }?;
            match unit {
                TimeUnit::Second => Some(ScalarValue::TimestampSecond(
                    Some(timestamp),
                    timezone.clone(),
                )),
                TimeUnit::Millisecond => Some(ScalarValue::TimestampMillisecond(
                    Some(timestamp),
                    timezone.clone(),
                )),
                TimeUnit::Microsecond => Some(ScalarValue::TimestampMicrosecond(
                    Some(timestamp),
                    timezone.clone(),
                )),
                TimeUnit::Nanosecond => Some(ScalarValue::TimestampNanosecond(
                    Some(timestamp),
                    timezone.clone(),
                )),
            }
        }
        _ => None,
    }
}

fn decimal_scalar(value: &DecimalNumber, data_type: &DataType) -> Option<ScalarValue> {
    let DataType::Decimal128(precision, target_scale) = data_type else {
        return None;
    };
    let unscaled = signed_i128_from_be(value.unscaled_value().as_ref())?;
    let scale_delta = i32::from(*target_scale).checked_sub(value.scale())?;
    let adjusted = if scale_delta >= 0 {
        unscaled.checked_mul(10_i128.checked_pow(scale_delta as u32)?)?
    } else {
        let divisor = 10_i128.checked_pow(scale_delta.unsigned_abs())?;
        if unscaled % divisor != 0 {
            return None;
        }
        unscaled / divisor
    };
    let limit = 10_u128.checked_pow(u32::from(*precision))?;
    if adjusted.unsigned_abs() >= limit {
        return None;
    }
    Some(ScalarValue::Decimal128(
        Some(adjusted),
        *precision,
        *target_scale,
    ))
}

fn signed_i128_from_be(bytes: &[u8]) -> Option<i128> {
    let first = *bytes.first()?;
    let sign_byte = if first & 0x80 == 0 { 0 } else { 0xff };
    let bytes = if bytes.len() > 16 {
        let extra = bytes.len() - 16;
        bytes[..extra]
            .iter()
            .all(|byte| *byte == sign_byte)
            .then_some(&bytes[extra..])?
    } else {
        bytes
    };
    if bytes.len() == 16 && ((bytes[0] & 0x80 == 0) != (sign_byte == 0)) {
        return None;
    }
    let mut buffer = [sign_byte; 16];
    buffer[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(buffer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_glue::primitives::{Blob, DateTime};
    use aws_sdk_glue::types::{
        BooleanColumnStatisticsData, ColumnStatisticsData, DateColumnStatisticsData,
        DecimalColumnStatisticsData, DoubleColumnStatisticsData, LongColumnStatisticsData,
        StringColumnStatisticsData,
    };
    use datafusion::arrow::datatypes::Schema;

    fn glue_statistics(
        name: &str,
        column_type: &str,
        data: ColumnStatisticsData,
    ) -> GlueColumnStatistics {
        GlueColumnStatistics::builder()
            .column_name(name)
            .column_type(column_type)
            .analyzed_time(DateTime::from_secs(0))
            .statistics_data(data)
            .build()
            .unwrap()
    }

    #[test]
    fn converts_long_statistics_to_arrow_integer_type() {
        let data = LongColumnStatisticsData::builder()
            .minimum_value(1)
            .maximum_value(100)
            .number_of_nulls(2)
            .number_of_distinct_values(80)
            .build();
        let statistics = glue_statistics(
            "id",
            "int",
            ColumnStatisticsData::builder()
                .r#type(ColumnStatisticsType::Long)
                .long_column_statistics_data(data)
                .build()
                .unwrap(),
        );

        let converted =
            convert_glue_column_statistics(&Field::new("id", DataType::Int32, true), &statistics);

        assert_eq!(converted.null_count, Precision::Inexact(2));
        assert_eq!(converted.distinct_count, Precision::Inexact(80));
        assert_eq!(
            converted.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(1)))
        );
        assert_eq!(
            converted.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(100)))
        );
    }

    #[test]
    fn converts_boolean_and_string_statistics() {
        let boolean_data = BooleanColumnStatisticsData::builder()
            .number_of_trues(10)
            .number_of_falses(5)
            .number_of_nulls(1)
            .build();
        let boolean_statistics = glue_statistics(
            "flag",
            "boolean",
            ColumnStatisticsData::builder()
                .r#type(ColumnStatisticsType::Boolean)
                .boolean_column_statistics_data(boolean_data)
                .build()
                .unwrap(),
        );
        let converted = convert_glue_column_statistics(
            &Field::new("flag", DataType::Boolean, true),
            &boolean_statistics,
        );
        assert_eq!(converted.distinct_count, Precision::Inexact(2));
        assert_eq!(
            converted.min_value,
            Precision::Inexact(ScalarValue::Boolean(Some(false)))
        );

        let string_data = StringColumnStatisticsData::builder()
            .maximum_length(10)
            .average_length(3.0)
            .number_of_nulls(4)
            .number_of_distinct_values(20)
            .build();
        let string_statistics = glue_statistics(
            "name",
            "string",
            ColumnStatisticsData::builder()
                .r#type(ColumnStatisticsType::String)
                .string_column_statistics_data(string_data)
                .build()
                .unwrap(),
        );
        let converted = convert_glue_column_statistics(
            &Field::new("name", DataType::Utf8, true),
            &string_statistics,
        );
        assert_eq!(converted.null_count, Precision::Inexact(4));
        assert_eq!(converted.distinct_count, Precision::Inexact(20));
        assert_eq!(converted.byte_size, Precision::Absent);
    }

    #[test]
    fn converts_date_decimal_and_double_statistics() {
        let date_data = DateColumnStatisticsData::builder()
            .minimum_value(DateTime::from_secs(86_400))
            .maximum_value(DateTime::from_secs(172_800))
            .number_of_nulls(0)
            .number_of_distinct_values(2)
            .build();
        let date_statistics = glue_statistics(
            "date",
            "date",
            ColumnStatisticsData::builder()
                .r#type(ColumnStatisticsType::Date)
                .date_column_statistics_data(date_data)
                .build()
                .unwrap(),
        );
        let converted = convert_glue_column_statistics(
            &Field::new("date", DataType::Date32, true),
            &date_statistics,
        );
        assert_eq!(
            converted.min_value,
            Precision::Inexact(ScalarValue::Date32(Some(1)))
        );

        let minimum = DecimalNumber::builder()
            .unscaled_value(Blob::new(1234_i16.to_be_bytes()))
            .scale(2)
            .build()
            .unwrap();
        let maximum = DecimalNumber::builder()
            .unscaled_value(Blob::new(5678_i16.to_be_bytes()))
            .scale(2)
            .build()
            .unwrap();
        let decimal_data = DecimalColumnStatisticsData::builder()
            .minimum_value(minimum)
            .maximum_value(maximum)
            .number_of_nulls(0)
            .number_of_distinct_values(10)
            .build();
        let decimal_statistics = glue_statistics(
            "price",
            "decimal(10,2)",
            ColumnStatisticsData::builder()
                .r#type(ColumnStatisticsType::Decimal)
                .decimal_column_statistics_data(decimal_data)
                .build()
                .unwrap(),
        );
        let converted = convert_glue_column_statistics(
            &Field::new("price", DataType::Decimal128(10, 2), true),
            &decimal_statistics,
        );
        assert_eq!(
            converted.min_value,
            Precision::Inexact(ScalarValue::Decimal128(Some(1234), 10, 2))
        );

        let double_data = DoubleColumnStatisticsData::builder()
            .minimum_value(1.5)
            .maximum_value(9.5)
            .number_of_nulls(0)
            .number_of_distinct_values(5)
            .build();
        let double_statistics = glue_statistics(
            "ratio",
            "double",
            ColumnStatisticsData::builder()
                .r#type(ColumnStatisticsType::Double)
                .double_column_statistics_data(double_data)
                .build()
                .unwrap(),
        );
        let converted = convert_glue_column_statistics(
            &Field::new("ratio", DataType::Float64, true),
            &double_statistics,
        );
        assert_eq!(
            converted.max_value,
            Precision::Inexact(ScalarValue::Float64(Some(9.5)))
        );
    }

    #[test]
    fn invalid_values_remain_absent() {
        assert_eq!(inexact_count(-1), Precision::Absent);
        assert!(signed_i128_from_be(&[]).is_none());
        assert!(scalar_from_i64(256, &DataType::Int8).is_none());
        assert!(scalar_from_f64(f64::NAN, &DataType::Float64).is_none());
    }

    #[test]
    fn aligns_statistics_to_full_table_schema() {
        let table_schema = TableSchema::new(
            std::sync::Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![std::sync::Arc::new(Field::new("dt", DataType::Utf8, true))],
        );
        let name_data = StringColumnStatisticsData::builder()
            .maximum_length(10)
            .average_length(3.0)
            .number_of_nulls(1)
            .number_of_distinct_values(20)
            .build();
        let id_data = LongColumnStatisticsData::builder()
            .minimum_value(1)
            .maximum_value(100)
            .number_of_nulls(0)
            .number_of_distinct_values(80)
            .build();
        let glue_statistics = vec![
            glue_statistics(
                "name",
                "string",
                ColumnStatisticsData::builder()
                    .r#type(ColumnStatisticsType::String)
                    .string_column_statistics_data(name_data)
                    .build()
                    .unwrap(),
            ),
            glue_statistics(
                "id",
                "bigint",
                ColumnStatisticsData::builder()
                    .r#type(ColumnStatisticsType::Long)
                    .long_column_statistics_data(id_data)
                    .build()
                    .unwrap(),
            ),
        ];

        let converted = convert_glue_columns_statistics(&table_schema, &glue_statistics);

        assert_eq!(converted.len(), 3);
        assert_eq!(converted[0].distinct_count, Precision::Inexact(80));
        assert_eq!(converted[1].distinct_count, Precision::Inexact(20));
        assert_eq!(converted[2], ColumnStatistics::new_unknown());
    }

    #[test]
    fn batches_column_names_at_glue_limit() {
        let column_names = (0..201).map(|index| format!("c{index}")).collect();

        let batches = batch_column_names(column_names);

        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            [100, 100, 1]
        );
    }
}
