use crate::sql::column_type::sql_type_name;
use crate::table_format::TableFormat;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::ResolvedTableReference;

#[derive(Debug)]
pub struct TableDefinitionBuilder {
    table_reference: ResolvedTableReference,
    table_location: String,
    table_format: TableFormat,
    table_schema: Schema,
    partition_column_names: Vec<String>,
}

impl TableDefinitionBuilder {
    pub fn new(
        table_reference: ResolvedTableReference,
        table_location: impl Into<String>,
        table_format: TableFormat,
        table_schema: Schema,
    ) -> Self {
        Self {
            table_reference,
            table_location: table_location.into(),
            table_format,
            table_schema,
            partition_column_names: vec![],
        }
    }

    pub fn with_partition_column_names(mut self, partition_column_names: Vec<String>) -> Self {
        self.partition_column_names = partition_column_names;
        self
    }

    pub fn build(self) -> String {
        let mut definition = format!(
            "CREATE TABLE `{}`.`{}`.`{}`\n(\n",
            escape_backtick_identifier(&self.table_reference.catalog),
            escape_backtick_identifier(&self.table_reference.schema),
            escape_backtick_identifier(&self.table_reference.table)
        );

        let columns = self
            .table_schema
            .fields()
            .iter()
            .map(|field| {
                format!(
                    "  `{}` {}",
                    escape_backtick_identifier(field.name()),
                    sql_type_name(field.data_type())
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");
        definition.push_str(&columns);
        definition.push_str("\n)\n");

        if !self.partition_column_names.is_empty() {
            let partition_columns = self
                .partition_column_names
                .iter()
                .map(|name| format!("`{}`", escape_backtick_identifier(name)))
                .collect::<Vec<_>>()
                .join(", ");
            definition.push_str(&format!("PARTITIONED BY ({partition_columns})\n"));
        }

        definition.push_str(&format!("USING {}\n", table_format_sql(self.table_format)));
        definition.push_str(&format!(
            "LOCATION '{}'",
            escape_sql_string(&self.table_location)
        ));
        definition
    }
}

fn table_format_sql(table_format: TableFormat) -> &'static str {
    match table_format {
        TableFormat::Hive => "HIVE",
        TableFormat::Iceberg => "ICEBERG",
        TableFormat::Delta => "DELTA",
        TableFormat::Paimon => "PAIMON",
    }
}

fn escape_backtick_identifier(value: &str) -> String {
    value.replace('`', "``")
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_build_hive_definition_with_partitions() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("dt", DataType::Utf8, true),
        ]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "catalog".into(),
                schema: "schema".into(),
                table: "table".into(),
            },
            "s3://bucket/path",
            TableFormat::Hive,
            schema,
        )
        .with_partition_column_names(vec!["dt".to_string()])
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` BIGINT,\n  `name` STRING,\n  `amount` DECIMAL(10, 2),\n  `dt` STRING\n)\nPARTITIONED BY (`dt`)\nUSING HIVE\nLOCATION 's3://bucket/path'"
        );
    }

    #[test]
    fn test_build_hive_definition_without_partitions() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "catalog".into(),
                schema: "schema".into(),
                table: "table".into(),
            },
            "s3://bucket/path",
            TableFormat::Hive,
            schema,
        )
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` BIGINT\n)\nUSING HIVE\nLOCATION 's3://bucket/path'"
        );
    }

    #[test]
    fn test_build_hive_definition_escapes_identifiers_and_location() {
        let schema = Schema::new(vec![Field::new("a`b", DataType::Utf8, true)]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "cat`alog".into(),
                schema: "schema".into(),
                table: "ta`ble".into(),
            },
            "s3://bucket/o'hara",
            TableFormat::Hive,
            schema,
        )
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `cat``alog`.`schema`.`ta``ble`\n(\n  `a``b` STRING\n)\nUSING HIVE\nLOCATION 's3://bucket/o''hara'"
        );
    }

    #[test]
    fn test_build_iceberg_definition_without_partitions() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "catalog".into(),
                schema: "schema".into(),
                table: "table".into(),
            },
            "s3://bucket/path",
            TableFormat::Iceberg,
            schema,
        )
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` BIGINT\n)\nUSING ICEBERG\nLOCATION 's3://bucket/path'"
        );
    }

    #[test]
    fn test_build_iceberg_definition_with_partitions() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("dt", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "catalog".into(),
                schema: "schema".into(),
                table: "table".into(),
            },
            "s3://bucket/path",
            TableFormat::Iceberg,
            schema,
        )
        .with_partition_column_names(vec!["dt".to_string(), "id".to_string()])
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` BIGINT,\n  `dt` STRING,\n  `amount` DECIMAL(10, 2)\n)\nPARTITIONED BY (`dt`, `id`)\nUSING ICEBERG\nLOCATION 's3://bucket/path'"
        );
    }

    #[test]
    fn test_build_iceberg_definition_escapes_identifiers_and_location() {
        let schema = Schema::new(vec![Field::new("a`b", DataType::Utf8, true)]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "cat`alog".into(),
                schema: "schema".into(),
                table: "ta`ble".into(),
            },
            "s3://bucket/o'hara",
            TableFormat::Iceberg,
            schema,
        )
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `cat``alog`.`schema`.`ta``ble`\n(\n  `a``b` STRING\n)\nUSING ICEBERG\nLOCATION 's3://bucket/o''hara'"
        );
    }

    #[test]
    fn test_build_delta_definition_without_partitions() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "catalog".into(),
                schema: "schema".into(),
                table: "table".into(),
            },
            "s3://bucket/path",
            TableFormat::Delta,
            schema,
        )
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` BIGINT\n)\nUSING DELTA\nLOCATION 's3://bucket/path'"
        );
    }

    #[test]
    fn test_build_delta_definition_with_partitions() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("dt", DataType::Utf8, true),
        ]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "catalog".into(),
                schema: "schema".into(),
                table: "table".into(),
            },
            "s3://bucket/path",
            TableFormat::Delta,
            schema,
        )
        .with_partition_column_names(vec!["dt".to_string()])
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` BIGINT,\n  `dt` STRING\n)\nPARTITIONED BY (`dt`)\nUSING DELTA\nLOCATION 's3://bucket/path'"
        );
    }

    #[test]
    fn test_build_paimon_definition_with_partitions() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("dt", DataType::Utf8, true),
        ]);

        let definition = TableDefinitionBuilder::new(
            ResolvedTableReference {
                catalog: "catalog".into(),
                schema: "schema".into(),
                table: "table".into(),
            },
            "hdfs://namenode:8020/warehouse/schema.db/table",
            TableFormat::Paimon,
            schema,
        )
        .with_partition_column_names(vec!["dt".to_string()])
        .build();

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` BIGINT,\n  `dt` STRING\n)\nPARTITIONED BY (`dt`)\nUSING PAIMON\nLOCATION 'hdfs://namenode:8020/warehouse/schema.db/table'"
        );
    }
}
