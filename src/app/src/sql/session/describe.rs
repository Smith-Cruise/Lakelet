use super::ExtendedSessionContext;
use crate::sql::column_type::sql_type_name;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::DescribeTable;
use std::sync::Arc;

impl ExtendedSessionContext {
    pub(super) fn describe_dataframe(&self, describe: &DescribeTable) -> Result<DataFrame> {
        let mut names = StringBuilder::new();
        let mut sql_types = StringBuilder::new();
        let mut data_types = StringBuilder::new();
        let mut nullables = StringBuilder::new();
        for field in describe.schema.fields() {
            names.append_value(field.name());
            sql_types.append_value(sql_type_name(field.data_type()));
            data_types.append_value(field.data_type().to_string());
            nullables.append_value(if field.is_nullable() { "YES" } else { "NO" });
        }
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("column_name", DataType::Utf8, false),
                Field::new("sql_type", DataType::Utf8, false),
                Field::new("data_type", DataType::Utf8, false),
                Field::new("is_nullable", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(names.finish()),
                Arc::new(sql_types.finish()),
                Arc::new(data_types.finish()),
                Arc::new(nullables.finish()),
            ],
        )?;
        self.session_context.read_batch(batch)
    }
}
