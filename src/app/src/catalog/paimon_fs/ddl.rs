use super::to_datafusion_error;
use crate::catalog::CreateTableSchema;
use datafusion::common::{Result, not_impl_err, plan_datafusion_err, plan_err};
use datafusion::logical_expr::sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
use datafusion::logical_expr::sqlparser::ast::{
    ArrayElemTypeDef, BinaryLength, CharacterLength, ColumnOption, CreateTable, CreateTableOptions,
    DataType as SqlType, ExactNumberInfo, Expr, HiveDistributionStyle, HiveIOFormat, Ident,
    PrimaryKeyConstraint, SqlOption, TableConstraint, TimezoneInfo, Value,
};
use datafusion::sql::planner::IdentNormalizer;
use paimon::spec::{
    ArrayType, BigIntType, BinaryType, BooleanType, CharType, DataField, DataType, DateType,
    DecimalType, DoubleType, FloatType, IntType, MapType, RowType, Schema, SmallIntType,
    TimestampType, TinyIntType, VarBinaryType, VarCharType,
};
use std::collections::{HashMap, HashSet};

pub(super) fn build_schema(
    stmt: &CreateTable,
    columns: &CreateTableSchema,
    normalize_identifiers: bool,
) -> Result<Schema> {
    validate_statement(stmt)?;
    if matches!(columns, CreateTableSchema::Declared) && stmt.columns.is_empty() {
        return plan_err!("CREATE TABLE requires at least one column");
    }
    let normalizer = IdentNormalizer::new(normalize_identifiers);
    let mut builder = Schema::builder();
    let mut column_names = HashSet::new();
    let mut explicitly_nullable = HashSet::new();
    let mut primary_keys = None;
    if let CreateTableSchema::Derived(schema) = columns {
        if !stmt.columns.is_empty() {
            return plan_err!("Derived table schemas cannot include column definitions");
        }
        if schema.fields().is_empty() {
            return plan_err!("CREATE TABLE requires at least one column");
        }
        for field in schema.fields() {
            if !column_names.insert(field.name().clone()) {
                return plan_err!("Duplicate column: {}", field.name());
            }
            let data_type =
                paimon::arrow::arrow_to_paimon_type(field.data_type(), field.is_nullable())
                    .map_err(to_datafusion_error)?;
            // Names are already resolved; do not normalize quoted query aliases again.
            // The builder assigns fresh IDs to top-level and nested fields.
            builder = builder.column(field.name(), data_type);
        }
    }
    for column in &stmt.columns {
        let name = normalize_name(&column.name, &normalizer)?;
        if !column_names.insert(name.clone()) {
            return plan_err!("Duplicate column: {name}");
        }
        let mut nullable = None;
        let mut comment = None;
        for option in &column.options {
            if option.name.is_some() {
                return not_impl_err!("Named column constraints are not supported");
            }
            match &option.option {
                ColumnOption::Null | ColumnOption::NotNull => {
                    let value = matches!(option.option, ColumnOption::Null);
                    if nullable.replace(value).is_some() {
                        return plan_err!("Repeated or conflicting nullability for column {name}");
                    }
                }
                ColumnOption::Comment(value) => {
                    if comment.replace(value.clone()).is_some() {
                        return plan_err!("Duplicate comment for column {name}");
                    }
                }
                ColumnOption::PrimaryKey(key) => {
                    validate_primary_key(key)?;
                    if !key.columns.is_empty() {
                        return not_impl_err!("Inline PRIMARY KEY cannot specify other columns");
                    }
                    set_primary_key(&mut primary_keys, vec![name.clone()])?;
                }
                other => return not_impl_err!("Unsupported column option: {other}"),
            }
        }
        if nullable == Some(true) {
            explicitly_nullable.insert(name.clone());
        }
        let data_type = convert_type(&column.data_type, nullable.unwrap_or(true), &normalizer)?;
        builder = builder.column_with_description(name, data_type, comment);
    }
    for constraint in &stmt.constraints {
        let TableConstraint::PrimaryKey(key) = constraint else {
            return not_impl_err!("Only PRIMARY KEY table constraints are supported");
        };
        validate_primary_key(key)?;
        let columns = key
            .columns
            .iter()
            .map(|column| {
                let Expr::Identifier(name) = &column.column.expr else {
                    return not_impl_err!("PRIMARY KEY requires simple column names");
                };
                if column.operator_class.is_some()
                    || column.column.options.asc.is_some()
                    || column.column.options.nulls_first.is_some()
                    || column.column.with_fill.is_some()
                {
                    return not_impl_err!("PRIMARY KEY index options are not supported");
                }
                normalize_name(name, &normalizer)
            })
            .collect::<Result<Vec<_>>>()?;
        if columns.is_empty() {
            return plan_err!("PRIMARY KEY requires at least one column");
        }
        set_primary_key(&mut primary_keys, columns)?;
    }
    let partition_keys = match &stmt.hive_distribution {
        HiveDistributionStyle::NONE => vec![],
        HiveDistributionStyle::PARTITIONED { columns } => columns
            .iter()
            .map(|column| {
                if column.data_type != SqlType::Unspecified || !column.options.is_empty() {
                    return plan_err!("PARTITIONED BY must only reference existing column names");
                }
                normalize_name(&column.name, &normalizer)
            })
            .collect::<Result<Vec<_>>>()?,
        _ => return not_impl_err!("Only PARTITIONED BY distribution is supported"),
    };
    let schema = builder
        .primary_key(primary_keys.unwrap_or_default())
        .partition_keys(partition_keys)
        .options(table_options(&stmt.table_options)?)
        .build()
        .map_err(to_datafusion_error)?;
    // Paimon also accepts primary keys through properties. Check the effective keys.
    for key in schema.primary_keys() {
        if explicitly_nullable.contains(key) {
            return plan_err!("PRIMARY KEY column {key} cannot be declared NULL");
        }
    }
    Ok(schema)
}

fn validate_statement(stmt: &CreateTable) -> Result<()> {
    // Reconstruct only supported fields so new SQL modifiers cannot be silently ignored.
    let supported = CreateTableBuilder::new(stmt.name.clone())
        .columns(stmt.columns.clone())
        .constraints(stmt.constraints.clone())
        .if_not_exists(stmt.if_not_exists)
        .hive_distribution(stmt.hive_distribution.clone())
        .hive_formats(stmt.hive_formats.clone())
        .table_options(stmt.table_options.clone())
        .build();
    if *stmt != supported {
        return not_impl_err!(
            "Paimon CREATE TABLE only supports column definitions, PRIMARY KEY, PARTITIONED BY, USING and WITH properties"
        );
    }
    if let Some(format) = &stmt.hive_formats
        && (format.row_format.is_some()
            || format.serde_properties.is_some()
            || format.location.is_some()
            || !matches!(format.storage, None | Some(HiveIOFormat::Using { .. })))
    {
        return not_impl_err!("Only USING is supported for Paimon table storage");
    }
    Ok(())
}

fn validate_primary_key(key: &PrimaryKeyConstraint) -> Result<()> {
    if key.name.is_some()
        || key.index_name.is_some()
        || key.index_type.is_some()
        || !key.index_options.is_empty()
        || key.characteristics.as_ref().is_some_and(|characteristics| {
            characteristics.deferrable.is_some() || characteristics.initially.is_some()
        })
    {
        return not_impl_err!("Unsupported PRIMARY KEY constraint options");
    }
    Ok(())
}

fn set_primary_key(keys: &mut Option<Vec<String>>, columns: Vec<String>) -> Result<()> {
    if keys.replace(columns).is_some() {
        return plan_err!("Only one PRIMARY KEY declaration is allowed");
    }
    Ok(())
}

fn table_options(options: &CreateTableOptions) -> Result<HashMap<String, String>> {
    let options = match options {
        CreateTableOptions::None => return Ok(HashMap::new()),
        CreateTableOptions::With(options) => options,
        _ => return not_impl_err!("Use WITH ('key' = 'value') for Paimon table properties"),
    };
    let mut properties = HashMap::new();
    for option in options {
        let SqlOption::KeyValue {
            key,
            value: Expr::Value(value),
        } = option
        else {
            return plan_err!("Table properties require string keys and values");
        };
        let Value::SingleQuotedString(value) = &value.value else {
            return plan_err!("Table property {} requires a string value", key.value);
        };
        if key.quote_style != Some('\'') {
            return plan_err!("Table property keys must be single-quoted strings");
        }
        if properties
            .insert(key.value.clone(), value.clone())
            .is_some()
        {
            return plan_err!("Duplicate table property: {}", key.value);
        }
    }
    Ok(properties)
}

fn normalize_name(name: &Ident, normalizer: &IdentNormalizer) -> Result<String> {
    let name = normalizer.normalize(name.clone());
    if name.trim().is_empty() {
        return plan_err!("Column names must not be empty");
    }
    Ok(name)
}

fn checked_u32(value: u64) -> Result<u32> {
    value
        .try_into()
        .map_err(|_| plan_datafusion_err!("Type parameter {value} exceeds the supported range"))
}

fn character_length(length: &Option<CharacterLength>, default: u32) -> Result<u32> {
    match length {
        None => Ok(default),
        Some(CharacterLength::IntegerLength { length, unit: None }) => checked_u32(*length),
        _ => not_impl_err!("Only integer character lengths are supported"),
    }
}

fn convert_type(
    sql_type: &SqlType,
    nullable: bool,
    normalizer: &IdentNormalizer,
) -> Result<DataType> {
    let data_type = match sql_type {
        SqlType::Boolean | SqlType::Bool => DataType::Boolean(BooleanType::with_nullable(nullable)),
        SqlType::TinyInt(None) => DataType::TinyInt(TinyIntType::with_nullable(nullable)),
        SqlType::SmallInt(None) => DataType::SmallInt(SmallIntType::with_nullable(nullable)),
        SqlType::Int(None) | SqlType::Integer(None) => {
            DataType::Int(IntType::with_nullable(nullable))
        }
        SqlType::BigInt(None) | SqlType::Int64 => {
            DataType::BigInt(BigIntType::with_nullable(nullable))
        }
        SqlType::Float(ExactNumberInfo::None) | SqlType::Real => {
            DataType::Float(FloatType::with_nullable(nullable))
        }
        SqlType::Double(ExactNumberInfo::None) | SqlType::DoublePrecision => {
            DataType::Double(DoubleType::with_nullable(nullable))
        }
        SqlType::Char(length) | SqlType::Character(length) => DataType::Char(
            CharType::with_nullable(nullable, character_length(length, 1)? as usize)
                .map_err(to_datafusion_error)?,
        ),
        SqlType::Varchar(length)
        | SqlType::CharacterVarying(length)
        | SqlType::CharVarying(length) => DataType::VarChar(
            VarCharType::with_nullable(
                nullable,
                character_length(length, VarCharType::MAX_LENGTH)?,
            )
            .map_err(to_datafusion_error)?,
        ),
        SqlType::String(None) => DataType::VarChar(
            VarCharType::with_nullable(nullable, VarCharType::MAX_LENGTH)
                .map_err(to_datafusion_error)?,
        ),
        SqlType::Binary(Some(length)) => DataType::Binary(
            BinaryType::with_nullable(nullable, checked_u32(*length)? as usize)
                .map_err(to_datafusion_error)?,
        ),
        SqlType::Binary(None) | SqlType::Varbinary(None) => DataType::VarBinary(
            VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH)
                .map_err(to_datafusion_error)?,
        ),
        SqlType::Varbinary(Some(BinaryLength::IntegerLength { length })) => DataType::VarBinary(
            VarBinaryType::try_new(nullable, checked_u32(*length)?).map_err(to_datafusion_error)?,
        ),
        SqlType::Date => DataType::Date(DateType::with_nullable(nullable)),
        SqlType::Timestamp(precision, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            DataType::Timestamp(
                TimestampType::with_nullable(nullable, checked_u32(precision.unwrap_or(6))?)
                    .map_err(to_datafusion_error)?,
            )
        }
        SqlType::Decimal(info) | SqlType::Numeric(info) | SqlType::Dec(info) => {
            let (precision, scale) = match info {
                ExactNumberInfo::None => (10, 0),
                ExactNumberInfo::Precision(precision) => (checked_u32(*precision)?, 0),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => (
                    checked_u32(*precision)?,
                    u32::try_from(*scale)
                        .map_err(|_| plan_datafusion_err!("Invalid DECIMAL scale: {scale}"))?,
                ),
            };
            DataType::Decimal(
                DecimalType::with_nullable(nullable, precision, scale)
                    .map_err(to_datafusion_error)?,
            )
        }
        SqlType::Array(
            ArrayElemTypeDef::AngleBracket(element)
            | ArrayElemTypeDef::SquareBracket(element, None),
        ) => DataType::Array(ArrayType::with_nullable(
            nullable,
            convert_type(element, true, normalizer)?,
        )),
        SqlType::Map(key, value) => DataType::Map(MapType::with_nullable(
            nullable,
            convert_type(key, false, normalizer)?,
            convert_type(value, true, normalizer)?,
        )),
        SqlType::Struct(fields, _) => {
            if fields.is_empty() {
                return plan_err!("STRUCT requires at least one named field");
            }
            let mut names = HashSet::new();
            let fields = fields
                .iter()
                .enumerate()
                .map(|(id, field)| {
                    if field.options.is_some() {
                        return not_impl_err!("STRUCT field options are not supported");
                    }
                    let name = field
                        .field_name
                        .as_ref()
                        .ok_or_else(|| plan_datafusion_err!("STRUCT fields must be named"))?;
                    let name = normalize_name(name, normalizer)?;
                    if !names.insert(name.clone()) {
                        return plan_err!("Duplicate STRUCT field: {name}");
                    }
                    let id = i32::try_from(id)
                        .map_err(|_| plan_datafusion_err!("Too many STRUCT fields"))?;
                    Ok(DataField::new(
                        id,
                        name,
                        convert_type(&field.field_type, true, normalizer)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            DataType::Row(RowType::with_nullable(nullable, fields))
        }
        _ => return not_impl_err!("Unsupported Paimon SQL type: {sql_type}"),
    };
    Ok(data_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ExtendedParser;
    use crate::statements::ExtendedStatement;
    use datafusion::common::DataFusionError;
    use sqlparser::ast::Statement;

    fn parse_schema(sql: &str, normalize: bool) -> Result<Schema> {
        let statements = ExtendedParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let ExtendedStatement::SQLStatement(statement) = &statements[0];
        let Statement::CreateTable(create) = statement.as_ref() else {
            panic!("expected CREATE TABLE: {sql}");
        };
        build_schema(create, &CreateTableSchema::Declared, normalize)
    }

    #[test]
    fn test_column_types_and_nullability() {
        for (sql_type, expected) in [
            ("BIGINT", "BIGINT"),
            ("INT64", "BIGINT"),
            ("DECIMAL", "DECIMAL(10, 0)"),
            ("DECIMAL(18)", "DECIMAL(18, 0)"),
            ("DECIMAL(18, 2)", "DECIMAL(18, 2)"),
            ("TIMESTAMP", "TIMESTAMP(6)"),
            ("TIMESTAMP(3)", "TIMESTAMP(3)"),
            ("VARCHAR(20)", "VARCHAR(20)"),
            ("BINARY(8)", "BINARY(8)"),
            ("DATE", "DATE"),
        ] {
            for (suffix, nullable) in [("", true), (" NULL", true), (" NOT NULL", false)] {
                let sql = format!("CREATE TABLE t (c {sql_type}{suffix})");
                let schema = parse_schema(&sql, true).unwrap();
                let data_type = schema.fields()[0].data_type();
                assert_eq!(data_type.is_nullable(), nullable, "{sql}");
                let expected = format!("{expected}{}", if nullable { "" } else { " NOT NULL" });
                assert_eq!(data_type.to_string(), expected, "{sql}");
            }
        }
    }

    #[test]
    fn test_nested_types_and_identifier_normalization() {
        for normalize in [true, false] {
            let schema = parse_schema(
                "CREATE TABLE t (ID BIGINT COMMENT 'identifier', `Kept` STRING,
                 Details STRUCT<City STRING, `Zip` INT>, Tags ARRAY<INT>,
                 Attributes MAP<STRING, ARRAY<DECIMAL(10, 2)>>)",
                normalize,
            )
            .unwrap();
            assert_eq!(
                schema.fields()[0].name(),
                if normalize { "id" } else { "ID" }
            );
            assert_eq!(schema.fields()[0].description(), Some("identifier"));
            assert_eq!(schema.fields()[1].name(), "Kept");
            let DataType::Row(row) = schema.fields()[2].data_type() else {
                panic!("expected nested row");
            };
            assert_eq!(
                row.fields()[0].name(),
                if normalize { "city" } else { "City" }
            );
            assert_eq!(row.fields()[1].name(), "Zip");
            assert!(row.fields().iter().all(|f| f.data_type().is_nullable()));
            let DataType::Array(array) = schema.fields()[3].data_type() else {
                panic!("expected array");
            };
            assert_eq!(array.element_type(), &DataType::Int(IntType::new()));
            let DataType::Map(map) = schema.fields()[4].data_type() else {
                panic!("expected map");
            };
            assert!(!map.key_type().is_nullable());
            assert_eq!(map.value_type().to_string(), "ARRAY<DECIMAL(10, 2)>");
        }
    }

    #[test]
    fn test_primary_key_enforcement_is_ignored() {
        for inline in [true, false] {
            let schemas: Vec<_> = ["", " NOT ENFORCED", " ENFORCED"]
                .into_iter()
                .map(|enforcement| {
                    let columns = if inline {
                        format!("ID BIGINT PRIMARY KEY{enforcement}, dt STRING")
                    } else {
                        format!("ID BIGINT, dt STRING, PRIMARY KEY (ID){enforcement}")
                    };
                    parse_schema(&format!("CREATE TABLE t ({columns})"), true).unwrap()
                })
                .collect();
            for schema in &schemas {
                assert_eq!(schema.fields(), schemas[0].fields());
                assert_eq!(schema.primary_keys(), &["id"]);
                assert!(!schema.fields()[0].data_type().is_nullable());
                assert!(schema.fields()[1].data_type().is_nullable());
            }
        }
        let schema = parse_schema(
            "CREATE TABLE t (ID BIGINT, DT STRING, PRIMARY KEY (ID, DT))
             PARTITIONED BY (DT) WITH ('bucket' = '2')",
            true,
        )
        .unwrap();
        assert_eq!(schema.primary_keys(), &["id", "dt"]);
        assert_eq!(schema.partition_keys(), &["dt"]);
        assert_eq!(
            schema.options().get("bucket").map(String::as_str),
            Some("2")
        );
    }

    #[test]
    fn test_invalid_definitions() {
        for (sql, message) in [
            ("CREATE TABLE t (ID INT, id INT)", "Duplicate column"),
            (
                "CREATE TABLE t (id INT PRIMARY KEY, PRIMARY KEY (id))",
                "Only one PRIMARY KEY",
            ),
            (
                "CREATE TABLE t (id INT NULL, PRIMARY KEY (id))",
                "cannot be declared NULL",
            ),
            (
                "CREATE TABLE t (id INT NULL) WITH ('primary-key' = 'id')",
                "cannot be declared NULL",
            ),
            (
                "CREATE TABLE t (id INT NULL NOT NULL)",
                "conflicting nullability",
            ),
            (
                "CREATE TABLE t (c STRUCT<X INT, x INT>)",
                "Duplicate STRUCT field",
            ),
            (
                "CREATE TABLE t (id INT) WITH ('bucket' = '1', 'bucket' = '2')",
                "Duplicate table property",
            ),
            (
                "CREATE TABLE t (id INT) WITH ('bucket' = 2)",
                "requires a string value",
            ),
            (
                "CREATE TABLE t (id INT) WITH (bucket = '2')",
                "keys must be single-quoted",
            ),
        ] {
            let error = parse_schema(sql, true).unwrap_err();
            assert!(matches!(error, DataFusionError::Plan(_)), "{sql}: {error}");
            assert!(error.to_string().contains(message), "{sql}: {error}");
        }
        // These constraints are validated by Paimon's schema builder/types.
        for sql in [
            "CREATE TABLE t (id INT, PRIMARY KEY (missing))",
            "CREATE TABLE t (id INT) PARTITIONED BY (missing)",
            "CREATE TABLE t (id DECIMAL(39, 2))",
            "CREATE TABLE t (id DECIMAL(10, 11))",
            "CREATE TABLE t (id TIMESTAMP(10))",
        ] {
            let error = parse_schema(sql, true).unwrap_err();
            assert!(
                matches!(error, DataFusionError::External(_)),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn test_unsupported_definitions() {
        for sql in [
            "CREATE TABLE t AS SELECT 1 AS id",
            "CREATE TABLE t (id INT DEFAULT 1)",
            "CREATE TABLE t (id INT UNIQUE)",
            "CREATE TABLE t (id INT, CONSTRAINT pk PRIMARY KEY (id))",
            "CREATE TABLE t (id INT) LOCATION '/tmp/unsupported'",
            "CREATE TABLE t (id INT) TBLPROPERTIES ('bucket' = '1')",
            "CREATE TABLE t (id TIMESTAMP WITH TIME ZONE)",
        ] {
            let error = parse_schema(sql, true).unwrap_err();
            assert!(
                matches!(error, DataFusionError::NotImplemented(_)),
                "{sql}: {error}"
            );
        }
    }
}
