use super::to_datafusion_error;
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

pub(super) fn build_schema(stmt: &CreateTable, normalize_identifiers: bool) -> Result<Schema> {
    validate_statement(stmt)?;
    if stmt.columns.is_empty() {
        return plan_err!("CREATE TABLE requires at least one column");
    }
    let normalizer = IdentNormalizer::new(normalize_identifiers);
    let mut builder = Schema::builder();
    let mut column_names = HashSet::new();
    let mut explicitly_nullable = HashSet::new();
    let mut primary_keys = None;
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
            characteristics.deferrable.is_some()
                || characteristics.initially.is_some()
                || characteristics.enforced == Some(true)
        })
    {
        return not_impl_err!("Only PRIMARY KEY with optional NOT ENFORCED is supported");
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
        SqlType::BigInt(None) => DataType::BigInt(BigIntType::with_nullable(nullable)),
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
