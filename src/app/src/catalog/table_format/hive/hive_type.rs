use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit, UnionFields, UnionMode};
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use std::sync::Arc;

/// Hive's default precision and scale for a bare `decimal`.
const DEFAULT_DECIMAL_PRECISION: u8 = 10;
const DEFAULT_DECIMAL_SCALE: i8 = 0;
const MAX_DECIMAL_PRECISION: u8 = 38;

/// Child field names follow what the parquet reader produces for standard
/// Spark and Hive files, so matching files need no conversion.
pub const LIST_ELEMENT_NAME: &str = "element";
pub const MAP_ENTRIES_NAME: &str = "key_value";
pub const MAP_KEY_NAME: &str = "key";
pub const MAP_VALUE_NAME: &str = "value";

/// Converts a Hive column type string, e.g. `array<struct<a:int,b:string>>`,
/// to the Arrow type it is read as.
pub fn hive_type_to_arrow_type(hive_type: &str) -> Result<DataType> {
    let mut parser = HiveTypeParser {
        input: hive_type,
        position: 0,
    };
    let data_type = parser.parse_type()?;
    if parser.position != hive_type.len() {
        return Err(parser.error("unexpected trailing characters"));
    }
    Ok(data_type)
}

struct HiveTypeParser<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> HiveTypeParser<'a> {
    fn parse_type(&mut self) -> Result<DataType> {
        let name = self.parse_token()?.to_ascii_lowercase();
        let data_type = match name.as_str() {
            "tinyint" => DataType::Int8,
            "smallint" => DataType::Int16,
            "int" => DataType::Int32,
            "bigint" => DataType::Int64,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "string" => DataType::Utf8,
            "varchar" | "char" => {
                self.expect('(')?;
                self.parse_number::<u32>()?;
                self.expect(')')?;
                DataType::Utf8
            }
            "binary" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "timestamp with local time zone" => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            "decimal" => self.parse_decimal()?,
            "void" => DataType::Null,
            "array" => {
                self.expect('<')?;
                let element = self.parse_type()?;
                self.expect('>')?;
                DataType::List(Arc::new(Field::new(LIST_ELEMENT_NAME, element, true)))
            }
            "map" => {
                self.expect('<')?;
                let key = self.parse_type()?;
                self.expect(',')?;
                let value = self.parse_type()?;
                self.expect('>')?;
                let entries = Fields::from(vec![
                    Field::new(MAP_KEY_NAME, key, false),
                    Field::new(MAP_VALUE_NAME, value, true),
                ]);
                DataType::Map(
                    Arc::new(Field::new(
                        MAP_ENTRIES_NAME,
                        DataType::Struct(entries),
                        false,
                    )),
                    false,
                )
            }
            "struct" => {
                self.expect('<')?;
                let mut fields = Vec::new();
                if self.consume('>') {
                    return Ok(DataType::Struct(fields.into()));
                }
                loop {
                    let name = self.parse_token()?;
                    self.expect(':')?;
                    fields.push(Field::new(name, self.parse_type()?, true));
                    if self.consume('>') {
                        break;
                    }
                    self.expect(',')?;
                }
                DataType::Struct(fields.into())
            }
            "uniontype" => {
                self.expect('<')?;
                let mut fields = Vec::new();
                loop {
                    fields.push(Field::new(
                        format!("_union_{}", fields.len()),
                        self.parse_type()?,
                        true,
                    ));
                    if self.consume('>') {
                        break;
                    }
                    self.expect(',')?;
                }
                // Same layout as the ORC reader produces for unions.
                DataType::Union(UnionFields::try_from_fields(fields)?, UnionMode::Sparse)
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported Hive column type: {}",
                    self.input
                )));
            }
        };
        Ok(data_type)
    }

    fn parse_decimal(&mut self) -> Result<DataType> {
        let (precision, scale) = if self.consume('(') {
            let precision = self.parse_number::<u8>()?;
            let scale = if self.consume(',') {
                self.parse_number::<i8>()?
            } else {
                DEFAULT_DECIMAL_SCALE
            };
            self.expect(')')?;
            (precision, scale)
        } else {
            (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE)
        };
        if precision == 0
            || precision > MAX_DECIMAL_PRECISION
            || scale < 0
            || scale as u8 > precision
        {
            return Err(self.error(&format!("invalid decimal({precision},{scale})")));
        }
        Ok(DataType::Decimal128(precision, scale))
    }

    fn parse_number<T: std::str::FromStr>(&mut self) -> Result<T> {
        let value = self.parse_token()?;
        value
            .parse()
            .map_err(|_| self.error(&format!("invalid number `{value}`")))
    }

    fn parse_token(&mut self) -> Result<&'a str> {
        let rest = &self.input[self.position..];
        let len = rest
            .find(|c: char| !c.is_alphanumeric() && !matches!(c, '_' | '.' | '$' | ' '))
            .unwrap_or(rest.len());
        if len == 0 {
            return Err(self.error("expected a name or number"));
        }
        self.position += len;
        Ok(&rest[..len])
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.input[self.position..].starts_with(expected) {
            self.position += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: char) -> Result<()> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(self.error(&format!("expected `{expected}`")))
        }
    }

    fn error(&self, message: &str) -> DataFusionError {
        DataFusionError::Plan(format!(
            "invalid Hive column type `{}` at position {}: {message}",
            self.input, self.position
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableDefinitionBuilder;
    use crate::table_format::TableFormat;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::TableReference;

    fn parse(hive_type: &str) -> DataType {
        hive_type_to_arrow_type(hive_type).unwrap()
    }

    fn list(element: DataType) -> DataType {
        DataType::List(Arc::new(Field::new(LIST_ELEMENT_NAME, element, true)))
    }

    fn map(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                MAP_ENTRIES_NAME,
                DataType::Struct(Fields::from(vec![
                    Field::new(MAP_KEY_NAME, key, false),
                    Field::new(MAP_VALUE_NAME, value, true),
                ])),
                false,
            )),
            false,
        )
    }

    fn structure(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect(),
        )
    }

    #[test]
    fn parses_primitive_types() {
        for (hive_type, expected) in [
            ("tinyint", DataType::Int8),
            ("smallint", DataType::Int16),
            ("int", DataType::Int32),
            ("INT", DataType::Int32),
            ("bIgInT", DataType::Int64),
            ("float", DataType::Float32),
            ("double", DataType::Float64),
            ("boolean", DataType::Boolean),
            ("string", DataType::Utf8),
            ("StRiNg", DataType::Utf8),
            ("varchar(20)", DataType::Utf8),
            ("CHAR(5)", DataType::Utf8),
            ("binary", DataType::Binary),
            ("date", DataType::Date32),
            ("void", DataType::Null),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "timestamp with local time zone",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                "TIMESTAMP WITH LOCAL TIME ZONE",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
        ] {
            assert_eq!(parse(hive_type), expected, "{hive_type}");
        }
    }

    #[test]
    fn parses_decimal_types() {
        assert_eq!(parse("decimal(10,2)"), DataType::Decimal128(10, 2));
        assert_eq!(parse("DeCiMaL(38,18)"), DataType::Decimal128(38, 18));
        assert_eq!(parse("decimal(1,0)"), DataType::Decimal128(1, 0));
        assert_eq!(parse("decimal(38,38)"), DataType::Decimal128(38, 38));
        assert_eq!(parse("decimal(5)"), DataType::Decimal128(5, 0));
        assert_eq!(parse("decimal"), DataType::Decimal128(10, 0));
        for invalid in [
            "decimal(0)",
            "decimal(39,2)",
            "decimal(256,0)",
            "decimal(5,6)",
            "decimal(5,-1)",
            "decimal(38,128)",
            "decimal(a,2)",
            "decimal(1,2,3)",
            "decimal()",
            "decimal(10,)",
            "decimal(10,2",
            "decimal( 10,2)",
            "decimal(10, 2)",
        ] {
            assert!(hive_type_to_arrow_type(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn parses_nested_types() {
        assert_eq!(parse("array<int>"), list(DataType::Int32));
        assert_eq!(
            parse("map<string,bigint>"),
            map(DataType::Utf8, DataType::Int64)
        );
        assert_eq!(
            parse("struct<a:int,b:string>"),
            structure(&[("a", DataType::Int32), ("b", DataType::Utf8)])
        );
        assert_eq!(
            parse("ARRAY<MAP<STRING,STRUCT<a:INT,b:ArRaY<string>>>>"),
            list(map(
                DataType::Utf8,
                structure(&[("a", DataType::Int32), ("b", list(DataType::Utf8))])
            ))
        );
        assert_eq!(
            parse("ARRAY<STRUCT<userId:INT>>"),
            list(structure(&[("userId", DataType::Int32)]))
        );
        assert_eq!(parse("struct<>"), structure(&[]));
        assert_eq!(parse("array<STRUCT<>>"), list(structure(&[])));
        assert_eq!(
            parse("struct<createdAt:TiMeStAmP,localTime:TIMESTAMP WITH LOCAL TIME ZONE>"),
            structure(&[
                (
                    "createdAt",
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                ),
                (
                    "localTime",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                )
            ])
        );
        assert_eq!(
            parse("struct<amount:decimal(10,2),tags:array<varchar(10)>>"),
            structure(&[
                ("amount", DataType::Decimal128(10, 2)),
                ("tags", list(DataType::Utf8))
            ])
        );
    }

    #[test]
    fn keeps_struct_field_names_as_written() {
        assert_eq!(
            parse(
                "STRUCT<userId:INT,UserID:STRING,first name:INT,a.b:INT,$value:STRING, padded :INT,用户ID:INT>"
            ),
            structure(&[
                ("userId", DataType::Int32),
                ("UserID", DataType::Utf8),
                ("first name", DataType::Int32),
                ("a.b", DataType::Int32),
                ("$value", DataType::Utf8),
                (" padded ", DataType::Int32),
                ("用户ID", DataType::Int32),
            ])
        );
    }

    #[test]
    fn parses_union_types() {
        let DataType::Union(fields, UnionMode::Sparse) = parse("UnIoNtYpE<INT,STRING>") else {
            panic!("expected a sparse union");
        };
        let variants: Vec<_> = fields
            .iter()
            .map(|(type_id, field)| (type_id, field.name().clone(), field.data_type().clone()))
            .collect();
        assert_eq!(
            variants,
            vec![
                (0, "_union_0".to_string(), DataType::Int32),
                (1, "_union_1".to_string(), DataType::Utf8),
            ]
        );

        let DataType::Union(fields, UnionMode::Sparse) =
            parse("uniontype<array<int>,struct<userId:string>>")
        else {
            panic!("expected a sparse union");
        };
        assert_eq!(
            fields.iter().next().unwrap().1.data_type(),
            &list(DataType::Int32)
        );
        assert_eq!(
            fields.iter().last().unwrap().1.data_type(),
            &structure(&[("userId", DataType::Utf8)])
        );

        let union_128 = format!("uniontype<{}>", vec!["int"; 128].join(","));
        let DataType::Union(fields, UnionMode::Sparse) = parse(&union_128) else {
            panic!("expected a sparse union");
        };
        assert_eq!(fields.len(), 128);
        assert_eq!(fields.iter().last().unwrap().0, 127);

        let union_129 = format!("uniontype<{}>", vec!["int"; 129].join(","));
        assert!(hive_type_to_arrow_type(&union_129).is_err());
    }

    #[test]
    fn shows_complex_hive_column_types() -> Result<()> {
        let schema = Schema::new(
            [
                ("tags", "array<string>"),
                ("props", "map<string,int>"),
                ("s", "STRUCT<userId:INT>"),
                ("u", "uniontype<int,string>"),
            ]
            .into_iter()
            .map(|(name, hive_type)| {
                Ok(Field::new(name, hive_type_to_arrow_type(hive_type)?, true))
            })
            .collect::<Result<Vec<_>>>()?,
        );

        let definition = TableDefinitionBuilder::new(
            TableReference::full("catalog", "schema", "table"),
            "s3://bucket/path",
            TableFormat::Hive,
            schema,
        )
        .build()?;

        assert!(definition.contains("`tags` List(Utf8, field: 'element')"));
        assert!(definition.contains(
            "`props` Map(\"key_value\": non-null Struct(\"key\": non-null Utf8, \"value\": Int32), unsorted)"
        ));
        assert!(definition.contains("`s` Struct(\"userId\": Int32)"));
        assert!(
            definition
                .contains("`u` Union(Sparse, 0: (\"_union_0\": Int32), 1: (\"_union_1\": Utf8))")
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_types() {
        let err = hive_type_to_arrow_type("interval_day_time").unwrap_err();
        assert!(
            err.to_string().contains("unsupported Hive column type"),
            "{err}"
        );
        for invalid in [
            "",
            "array<int",
            "array<>",
            "array<int,string>",
            "map<>",
            "map<string>",
            "map<string,int,int>",
            "uniontype<>",
            "uniontype<int,>",
            "struct<a:int,>",
            "struct<a int>",
            "struct<:int>",
            "struct<`a:int>",
            "struct<`a`:int>",
            "int>",
            "timestamp with time zone",
            "timestamp  with local time zone",
            "timestamp\twith local time zone",
            "array<unknown>",
            "integer",
            "long",
            "double precision",
            "binary string",
            " int",
            "int ",
            "array <int>",
            "array< int>",
            "array<int >",
            "array<int> ",
            "char",
            "varchar",
            "char()",
            "char(-1)",
            "varchar(a)",
            "varchar(1.5)",
            "varchar(10,20)",
            "varchar(10",
            "char( 5 )",
        ] {
            assert!(hive_type_to_arrow_type(invalid).is_err(), "{invalid}");
        }
    }
}
