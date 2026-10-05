use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, LargeListArray, ListArray, MapArray, StructArray, new_null_array,
};
use datafusion::arrow::compute::{can_cast_types, cast};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Resolves table columns against data file columns ignoring ASCII case.
///
/// Hive metastores store lowercase column names, while engines such as Spark
/// keep the original case in the files they write. Matching by exact name
/// would read those columns as NULL. Exact matches still win, and a column is
/// only resolved case-insensitively when exactly one file column matches.
///
/// The same rule applies to struct fields at any depth: a nested column whose
/// file type differs from the table type is converted by [`cast_nested`]
/// instead of DataFusion's case-sensitive struct cast.
#[derive(Debug, Default)]
pub struct CaseInsensitivePhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for CaseInsensitivePhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        // Mapping logicl file name -> physical file name
        let mut renames = HashMap::new();
        // Physical column name to the table field it is converted to.
        let mut nested_casts = HashMap::new();
        let mut inner_fields = Vec::with_capacity(logical_file_schema.fields().len());
        for logical_field in logical_file_schema.fields() {
            let Some(physical_field) =
                find_field(physical_file_schema.fields(), logical_field.name())
            else {
                // Not found, reader will fill null later
                inner_fields.push(Arc::clone(logical_field));
                continue;
            };
            if physical_field.name() != logical_field.name() {
                renames.insert(logical_field.name().clone(), physical_field.name().clone());
            }
            if supports_nested_cast(logical_field.data_type())
                && logical_field.data_type() != physical_field.data_type()
            {
                validate_nested_cast(
                    logical_field.name(),
                    physical_field.data_type(),
                    logical_field.data_type(),
                )?;
                nested_casts.insert(physical_field.name().clone(), Arc::clone(logical_field));
                // The default adapter then keeps the physical column as is.
                inner_fields.push(Arc::clone(physical_field));
            } else {
                inner_fields.push(Arc::new(
                    logical_field
                        .as_ref()
                        .clone()
                        .with_name(physical_field.name()),
                ));
            }
        }
        if renames.is_empty() && nested_casts.is_empty() {
            return DefaultPhysicalExprAdapterFactory
                .create(logical_file_schema, physical_file_schema);
        }

        // Rename the logical fields to their physical spelling, so the default
        // adapter resolves them and rewritten columns carry the physical name
        // that statistics pruning and row filters look up.
        let inner_logical_schema = Arc::new(Schema::new_with_metadata(
            inner_fields,
            logical_file_schema.metadata().clone(),
        ));
        let inner = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&inner_logical_schema), physical_file_schema)?;
        Ok(Arc::new(CaseInsensitivePhysicalExprAdapter {
            renames,
            nested_casts,
            inner_logical_schema,
            inner,
        }))
    }
}

#[derive(Debug)]
struct CaseInsensitivePhysicalExprAdapter {
    /// Logical column name to the physical column name it resolves to.
    renames: HashMap<String, String>,
    /// Physical column name to the table field it is converted to.
    nested_casts: HashMap<String, FieldRef>,
    /// The logical schema handed to `inner`: physical names, and physical
    /// types for the columns in `nested_casts`.
    inner_logical_schema: SchemaRef,
    inner: Arc<dyn PhysicalExprAdapter>,
}

impl CaseInsensitivePhysicalExprAdapter {
    fn physical_name<'a>(&'a self, column: &'a Column) -> &'a str {
        self.renames
            .get(column.name())
            .map_or(column.name(), String::as_str)
    }

    /// Rewrites `get_field(column, 'a', 'b')` on a converted struct column to
    /// the physical field path, e.g. `get_field(column, 'A', 'b')`.
    ///
    /// The Parquet scan only evaluates pushed-down filters of this shape and
    /// silently skips others, while filters were accepted as pushed down by
    /// checking the table schema. Wrapping the column instead would lose them.
    fn rewrite_struct_field_access(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let Some(func) = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())
        else {
            return Ok(None);
        };
        let args = func.args();
        let Some(column) = args.first().and_then(|arg| arg.downcast_ref::<Column>()) else {
            return Ok(None);
        };
        let name = self.physical_name(column);
        if !self.nested_casts.contains_key(name) {
            return Ok(None);
        }
        let Some(path) = args[1..]
            .iter()
            .map(|arg| {
                arg.downcast_ref::<Literal>()
                    .and_then(|literal| literal.value().try_as_str().flatten())
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };

        let target_type = func.return_type();
        let mut data_type = self.inner_logical_schema.field(column.index()).data_type();
        let mut physical_args: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::new(Column::new(name, column.index()))];
        for field_name in path {
            // Map key lookups also go through get_field; leave them wrapped.
            let DataType::Struct(fields) = data_type else {
                return Ok(None);
            };
            let Some(field) = find_field(fields, field_name) else {
                return Ok(Some(Arc::new(Literal::new(ScalarValue::try_from(
                    target_type,
                )?))));
            };
            physical_args.push(Arc::new(Literal::new(ScalarValue::from(
                field.name().as_str(),
            ))));
            data_type = field.data_type();
        }

        let access: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(func.fun().clone()),
            physical_args,
            &self.inner_logical_schema,
            Arc::new(func.config_options().clone()),
        )?);
        let access_type = access.data_type(&self.inner_logical_schema)?;
        Ok(Some(if &access_type == target_type {
            access
        } else if supports_nested_cast(target_type) {
            Arc::new(NestedCastExpr {
                expr: access,
                // The original access keeps the table type as its return field.
                target_field: func.return_field(&self.inner_logical_schema)?,
            })
        } else {
            Arc::new(CastExpr::new(access, target_type.clone(), None))
        }))
    }
}

impl PhysicalExprAdapter for CaseInsensitivePhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = expr
            .transform_down(|expr| {
                if let Some(access) = self.rewrite_struct_field_access(&expr)? {
                    return Ok(Transformed::new(access, true, TreeNodeRecursion::Jump));
                }
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let name = self.physical_name(column);
                let Some(target_field) = self.nested_casts.get(name) else {
                    if name == column.name() {
                        return Ok(Transformed::no(expr));
                    }
                    return Ok(Transformed::yes(Arc::new(Column::new(
                        name,
                        column.index(),
                    ))));
                };
                // Jump, so the wrapped column is not visited again.
                Ok(Transformed::new(
                    Arc::new(NestedCastExpr {
                        expr: Arc::new(Column::new(name, column.index())),
                        target_field: Arc::clone(target_field),
                    }),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            })
            .data()?;
        self.inner.rewrite(expr)
    }
}

/// Finds a field by exact name, falling back to the only field whose name
/// matches ignoring ASCII case.
fn find_field<'a>(fields: &'a Fields, name: &str) -> Option<&'a FieldRef> {
    find_field_index(fields, name).map(|index| &fields[index])
}

fn find_field_index(fields: &Fields, name: &str) -> Option<usize> {
    if let Some((index, _)) = fields.find(name) {
        return Some(index);
    }
    let mut matches = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name().eq_ignore_ascii_case(name));
    match (matches.next(), matches.next()) {
        (Some((index, _)), None) => Some(index),
        _ => None,
    }
}

fn supports_nested_cast(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    )
}

/// Checks up front that [`cast_nested`] can convert `source` to `target`, so
/// an incompatible file fails when it is opened.
fn validate_nested_cast(name: &str, source: &DataType, target: &DataType) -> Result<()> {
    match (source, target) {
        (DataType::Null, _) => Ok(()),
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            target_fields.iter().try_for_each(|target_field| {
                match find_field(source_fields, target_field.name()) {
                    Some(source_field) => validate_nested_cast(
                        &format!("{name}.{}", target_field.name()),
                        source_field.data_type(),
                        target_field.data_type(),
                    ),
                    None => Ok(()),
                }
            })
        }
        (DataType::List(source_field), DataType::List(target_field))
        | (DataType::LargeList(source_field), DataType::LargeList(target_field)) => {
            validate_nested_cast(name, source_field.data_type(), target_field.data_type())
        }
        (DataType::Map(source_entries, _), DataType::Map(target_entries, _)) => {
            let (source_key, source_value) = map_key_value(source_entries)?;
            let (target_key, target_value) = map_key_value(target_entries)?;
            validate_nested_cast(name, source_key.data_type(), target_key.data_type())?;
            validate_nested_cast(name, source_value.data_type(), target_value.data_type())
        }
        (source, target) if !supports_nested_cast(target) && can_cast_types(source, target) => {
            Ok(())
        }
        _ => Err(DataFusionError::Plan(format!(
            "cannot read column `{name}` of type {source} as {target}"
        ))),
    }
}

fn map_key_value(entries: &FieldRef) -> Result<(&FieldRef, &FieldRef)> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => Ok((&fields[0], &fields[1])),
        other => Err(DataFusionError::Internal(format!(
            "invalid map entries type {other}"
        ))),
    }
}

/// Converts a nested array to `target`, reusing the child buffers:
/// - struct fields are matched by name ignoring case, and missing fields are
///   filled with NULL;
/// - list and map children are converted recursively whatever their field
///   names are, e.g. `element`, `item` or the legacy `array`;
/// - other types use Arrow's cast.
pub(crate) fn cast_nested(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    if array.data_type() == &DataType::Null {
        return Ok(new_null_array(target, array.len()));
    }
    let array: ArrayRef = match target {
        DataType::Struct(target_fields) => {
            let source = array
                .as_struct_opt()
                .ok_or_else(|| cast_error(array, target))?;
            let columns = target_fields
                .iter()
                .map(
                    |target_field| match find_field_index(source.fields(), target_field.name()) {
                        Some(index) => cast_nested(source.column(index), target_field.data_type()),
                        None => Ok(new_null_array(target_field.data_type(), source.len())),
                    },
                )
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(
                target_fields.clone(),
                columns,
                source.nulls().cloned(),
            )?)
        }
        DataType::List(target_field) => {
            let source = array
                .as_list_opt::<i32>()
                .ok_or_else(|| cast_error(array, target))?;
            Arc::new(ListArray::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                cast_nested(source.values(), target_field.data_type())?,
                source.nulls().cloned(),
            )?)
        }
        DataType::LargeList(target_field) => {
            let source = array
                .as_list_opt::<i64>()
                .ok_or_else(|| cast_error(array, target))?;
            Arc::new(LargeListArray::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                cast_nested(source.values(), target_field.data_type())?,
                source.nulls().cloned(),
            )?)
        }
        DataType::Map(target_entries, sorted) => {
            let source = array
                .as_map_opt()
                .ok_or_else(|| cast_error(array, target))?;
            let DataType::Struct(target_entry_fields) = target_entries.data_type() else {
                return Err(cast_error(array, target));
            };
            let (target_key, target_value) = map_key_value(target_entries)?;
            let entries = source.entries();
            let entries = StructArray::try_new(
                target_entry_fields.clone(),
                vec![
                    cast_nested(entries.column(0), target_key.data_type())?,
                    cast_nested(entries.column(1), target_value.data_type())?,
                ],
                entries.nulls().cloned(),
            )?;
            Arc::new(MapArray::try_new(
                Arc::clone(target_entries),
                source.offsets().clone(),
                entries,
                source.nulls().cloned(),
                *sorted,
            )?)
        }
        _ => cast(array, target)?,
    };
    Ok(array)
}

fn cast_error(array: &ArrayRef, target: &DataType) -> DataFusionError {
    DataFusionError::Execution(format!("cannot convert {} to {target}", array.data_type()))
}

/// Converts a nested file column to its table type with [`cast_nested`].
#[derive(Debug, Eq)]
struct NestedCastExpr {
    expr: Arc<dyn PhysicalExpr>,
    target_field: FieldRef,
}

impl PartialEq for NestedCastExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.as_ref() == other.expr.as_ref() && self.target_field == other.target_field
    }
}

impl Hash for NestedCastExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.as_ref().hash(state);
        self.target_field.hash(state);
    }
}

impl fmt::Display for NestedCastExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NESTED_CAST({} AS {})",
            self.expr,
            self.target_field.data_type()
        )
    }
}

impl PhysicalExpr for NestedCastExpr {
    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let target = self.target_field.data_type();
        Ok(match self.expr.evaluate(batch)? {
            ColumnarValue::Array(array) => ColumnarValue::Array(cast_nested(&array, target)?),
            ColumnarValue::Scalar(scalar) => ColumnarValue::Scalar(ScalarValue::try_from_array(
                &cast_nested(&scalar.to_array()?, target)?,
                0,
            )?),
        })
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            expr: children.swap_remove(0),
            target_field: Arc::clone(&self.target_field),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST(")?;
        self.expr.fmt_sql(f)?;
        write!(f, " AS {})", self.target_field.data_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Int32Array, Int64Array, MapBuilder, StringArray, StringBuilder,
    };
    use datafusion::arrow::array::{Int32Builder, ListBuilder};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::Field;

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn rewrite_column(
        logical: &SchemaRef,
        physical: &SchemaRef,
        name: &str,
    ) -> Arc<dyn PhysicalExpr> {
        let adapter = CaseInsensitivePhysicalExprAdapterFactory
            .create(Arc::clone(logical), Arc::clone(physical))
            .unwrap();
        let index = logical.index_of(name).unwrap();
        adapter.rewrite(Arc::new(Column::new(name, index))).unwrap()
    }

    #[test]
    fn resolves_columns_ignoring_case() {
        let logical = schema(&[("id", DataType::Int64), ("userid", DataType::Int64)]);
        let physical = schema(&[("userId", DataType::Int64), ("id", DataType::Int64)]);

        let rewritten = rewrite_column(&logical, &physical, "userid");
        let column = rewritten.downcast_ref::<Column>().unwrap();
        assert_eq!((column.name(), column.index()), ("userId", 0));

        let rewritten = rewrite_column(&logical, &physical, "id");
        let column = rewritten.downcast_ref::<Column>().unwrap();
        assert_eq!((column.name(), column.index()), ("id", 1));
    }

    #[test]
    fn prefers_exact_match_and_skips_ambiguous_names() {
        // An exact match is used even when another column differs only by case.
        let logical = schema(&[("name", DataType::Utf8)]);
        let physical = schema(&[("NAME", DataType::Utf8), ("name", DataType::Utf8)]);
        let rewritten = rewrite_column(&logical, &physical, "name");
        let column = rewritten.downcast_ref::<Column>().unwrap();
        assert_eq!((column.name(), column.index()), ("name", 1));

        // Several case-insensitive candidates: treat the column as missing.
        let physical = schema(&[("NAME", DataType::Utf8), ("Name", DataType::Utf8)]);
        let rewritten = rewrite_column(&logical, &physical, "name");
        assert!(rewritten.downcast_ref::<Column>().is_none());
    }

    fn nullable(name: &str, data_type: DataType) -> FieldRef {
        Arc::new(Field::new(name, data_type, true))
    }

    fn struct_type(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(
            fields
                .iter()
                .map(|(name, data_type)| nullable(name, data_type.clone()))
                .collect(),
        )
    }

    fn map_type(entries: &str, key: &str, value: &str, value_type: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                entries,
                DataType::Struct(Fields::from(vec![
                    Field::new(key, DataType::Utf8, false),
                    Field::new(value, value_type, true),
                ])),
                false,
            )),
            false,
        )
    }

    #[test]
    fn cast_nested_matches_struct_fields_ignoring_case() {
        // File struct {userId, Extra, name}; row 1 is a NULL struct.
        let source: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![
                nullable("userId", DataType::Int32),
                nullable("Extra", DataType::Utf8),
                nullable("name", DataType::Utf8),
            ]),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
            Some(NullBuffer::from(vec![true, false])),
        ));
        let target = struct_type(&[
            ("name", DataType::Utf8),
            ("userid", DataType::Int64),
            ("missing", DataType::Int32),
        ]);

        let result = cast_nested(&source, &target).unwrap();
        assert_eq!(result.data_type(), &target);
        let result = result.as_struct();
        assert!(result.is_valid(0) && result.is_null(1));
        assert_eq!(
            result.column(0).as_string::<i32>(),
            &StringArray::from(vec!["a", "b"])
        );
        assert_eq!(
            result.column(1).as_primitive(),
            &Int64Array::from(vec![1, 2])
        );
        assert_eq!(result.column(2).null_count(), 2);
    }

    #[test]
    fn cast_nested_converts_list_and_map_children() {
        // Legacy list element name, and a struct inside a list.
        let mut builder =
            ListBuilder::new(Int32Builder::new()).with_field(nullable("array", DataType::Int32));
        builder.append_value([Some(1), None]);
        builder.append_null();
        let source: ArrayRef = Arc::new(builder.finish());
        let target = DataType::List(nullable("element", DataType::Int64));
        let result = cast_nested(&source, &target).unwrap();
        assert_eq!(result.data_type(), &target);
        let result = result.as_list::<i32>();
        assert!(result.is_null(1));
        assert_eq!(
            result.value(0).as_primitive(),
            &Int64Array::from(vec![Some(1), None])
        );

        // The value struct gained a field, and the entry names differ.
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("k");
        builder.values().append_value(7);
        builder.append(true).unwrap();
        let int_map: ArrayRef = Arc::new(builder.finish());
        let DataType::Map(entries, _) = int_map.data_type() else {
            unreachable!()
        };
        let DataType::Struct(entry_fields) = entries.data_type() else {
            unreachable!()
        };
        let map_entries = int_map.as_map().entries();
        let source: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    entry_fields[0].as_ref().clone(),
                    Field::new("values", struct_type(&[("A", DataType::Int32)]), true),
                ])),
                false,
            )),
            int_map.as_map().offsets().clone(),
            StructArray::new(
                Fields::from(vec![
                    entry_fields[0].as_ref().clone(),
                    Field::new("values", struct_type(&[("A", DataType::Int32)]), true),
                ]),
                vec![
                    Arc::clone(map_entries.column(0)),
                    Arc::new(StructArray::new(
                        Fields::from(vec![nullable("A", DataType::Int32)]),
                        vec![Arc::clone(map_entries.column(1))],
                        None,
                    )),
                ],
                None,
            ),
            None,
            false,
        ));
        let target = map_type(
            "key_value",
            "key",
            "value",
            struct_type(&[("a", DataType::Int32), ("b", DataType::Utf8)]),
        );
        let result = cast_nested(&source, &target).unwrap();
        assert_eq!(result.data_type(), &target);
        let values = result.as_map().values().as_struct().clone();
        assert_eq!(values.column(0).as_primitive(), &Int32Array::from(vec![7]));
        assert_eq!(values.column(1).null_count(), 1);
    }

    #[test]
    fn rewrites_nested_columns_and_rejects_incompatible_types() {
        let logical = schema(&[("s", struct_type(&[("userid", DataType::Int32)]))]);
        let physical = schema(&[("S", struct_type(&[("userId", DataType::Int32)]))]);
        let rewritten = rewrite_column(&logical, &physical, "s");
        let cast = rewritten.downcast_ref::<NestedCastExpr>().unwrap();
        assert_eq!(cast.target_field.data_type(), logical.field(0).data_type());
        let column = cast.expr.downcast_ref::<Column>().unwrap();
        assert_eq!((column.name(), column.index()), ("S", 0));

        // Matching nested types are left to the default adapter.
        let rewritten = rewrite_column(&logical, &logical, "s");
        assert!(rewritten.downcast_ref::<Column>().is_some());

        let physical = schema(&[(
            "s",
            struct_type(&[("userId", struct_type(&[("x", DataType::Int32)]))]),
        )]);
        let err = CaseInsensitivePhysicalExprAdapterFactory
            .create(logical, physical)
            .unwrap_err();
        assert!(err.to_string().contains("s.userid"), "{err}");
    }
}
