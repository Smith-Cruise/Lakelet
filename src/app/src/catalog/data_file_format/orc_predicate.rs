//! Conservative translation of DataFusion expressions into ORC pruning predicates.

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};
use datafusion::physical_expr_adapter::PhysicalExprAdapter;
use orc_rust::{ComparisonOp, Predicate, PredicateValue};
use std::sync::Arc;

/// Returns a necessary condition for the original expression, never an exact row filter.
pub(super) fn to_orc_predicate(
    expr: &Arc<dyn PhysicalExpr>,
    logical_schema: &Schema,
    physical_schema: &Schema,
    adapter: &dyn PhysicalExprAdapter,
) -> Option<Predicate> {
    let convert = |expr: &Arc<dyn PhysicalExpr>| {
        to_orc_predicate(expr, logical_schema, physical_schema, adapter)
    };
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        match binary.op() {
            Operator::And => {
                return combine_and([convert(binary.left()), convert(binary.right())]);
            }
            Operator::Or => {
                return Some(Predicate::or(vec![
                    convert(binary.left())?,
                    convert(binary.right())?,
                ]));
            }
            _ => {}
        }
        let mut op = comparison_op(*binary.op())?;
        let (column_expr, value) = if let Some(value) = literal_value(binary.right()) {
            (binary.left(), value)
        } else {
            let value = literal_value(binary.left())?;
            op = reverse(op);
            (binary.right(), value)
        };
        let (name, data_type) =
            adapted_column(column_expr, logical_schema, physical_schema, adapter)?;
        if data_type == DataType::Boolean
            && !matches!(op, ComparisonOp::Equal | ComparisonOp::NotEqual)
        {
            return None;
        }
        return Some(Predicate::comparison(
            &name,
            op,
            predicate_value(&value, &data_type)?,
        ));
    }
    if let Some(is_null) = expr.downcast_ref::<IsNullExpr>() {
        let (name, _) = adapted_column(is_null.arg(), logical_schema, physical_schema, adapter)?;
        return Some(Predicate::is_null(&name));
    }
    if let Some(is_not_null) = expr.downcast_ref::<IsNotNullExpr>() {
        let (name, _) =
            adapted_column(is_not_null.arg(), logical_schema, physical_schema, adapter)?;
        return Some(Predicate::is_not_null(&name));
    }
    if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
        if in_list.negated() {
            return None;
        }
        let (name, data_type) =
            adapted_column(in_list.expr(), logical_schema, physical_schema, adapter)?;
        let mut alternatives = Vec::new();
        for item in in_list.list() {
            let value = literal_value(item)?;
            // NULL cannot make a positive IN condition true.
            if !value.is_null() {
                alternatives.push(Predicate::eq(&name, predicate_value(&value, &data_type)?));
            }
        }
        return (!alternatives.is_empty()).then(|| Predicate::or(alternatives));
    }
    if let Some(not) = expr.downcast_ref::<NotExpr>() {
        // Never negate an AND that may have had unsupported branches removed.
        if not
            .arg()
            .downcast_ref::<BinaryExpr>()
            .is_none_or(|expr| comparison_op(*expr.op()).is_none())
            && !not.arg().is::<IsNullExpr>()
            && !not.arg().is::<IsNotNullExpr>()
            && !not.arg().is::<Column>()
        {
            return None;
        }
        return match convert(not.arg())? {
            Predicate::Comparison { column, op, value } => {
                Some(Predicate::comparison(&column, op.negate(), value))
            }
            Predicate::IsNull { column } => Some(Predicate::is_not_null(&column)),
            Predicate::IsNotNull { column } => Some(Predicate::is_null(&column)),
            _ => None,
        };
    }
    // DataFusion simplifies boolean equality to a bare column or NOT(column).
    if let Some((name, DataType::Boolean)) =
        adapted_column(expr, logical_schema, physical_schema, adapter)
    {
        return Some(Predicate::eq(&name, PredicateValue::Boolean(Some(true))));
    }
    None
}

pub(super) fn combine_and(
    predicates: impl IntoIterator<Item = Option<Predicate>>,
) -> Option<Predicate> {
    let mut predicates: Vec<_> = predicates.into_iter().flatten().collect();
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(Predicate::and(predicates)),
    }
}

pub(super) fn predicate_columns<'a>(predicate: &'a Predicate, columns: &mut Vec<&'a str>) {
    match predicate {
        Predicate::Comparison { column, .. }
        | Predicate::IsNull { column }
        | Predicate::IsNotNull { column } => columns.push(column),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                predicate_columns(child, columns);
            }
        }
        Predicate::Not(child) => predicate_columns(child, columns),
    }
}

fn adapted_column(
    expr: &Arc<dyn PhysicalExpr>,
    logical_schema: &Schema,
    physical_schema: &Schema,
    adapter: &dyn PhysicalExprAdapter,
) -> Option<(String, DataType)> {
    // Partition columns are outside the logical file schema. Reject them before
    // invoking the adapter, which expects file column indices only.
    column_without_lossy_cast(expr, logical_schema)?;
    let expr = adapter.rewrite(Arc::clone(expr)).ok()?;
    let column = column_without_lossy_cast(&expr, physical_schema)?;
    let field = physical_schema.fields().get(column.index())?;
    let data_type = field.data_type();
    if !matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Boolean
            | DataType::Utf8
    ) {
        return None;
    }
    Some((field.name().clone(), data_type.clone()))
}

fn column_without_lossy_cast<'a>(
    expr: &'a Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Option<&'a Column> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        schema.fields().get(column.index())?;
        return Some(column);
    }
    let cast = expr.downcast_ref::<CastExpr>()?;
    let source_type = cast.expr().data_type(schema).ok()?;
    if source_type != *cast.cast_type()
        && !matches!((integer_width(&source_type), integer_width(cast.cast_type())), (Some(source), Some(target)) if source <= target)
    {
        return None;
    }
    column_without_lossy_cast(cast.expr(), schema)
}

fn integer_width(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(8),
        DataType::Int16 => Some(16),
        DataType::Int32 => Some(32),
        DataType::Int64 => Some(64),
        _ => None,
    }
}

fn literal_value(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        return Some(literal.value().clone());
    }
    let cast = expr.downcast_ref::<CastExpr>()?;
    let value = literal_value(cast.expr())?;
    if value.is_null() {
        return ScalarValue::try_from(cast.cast_type()).ok();
    }
    if value.data_type() != *cast.cast_type()
        && !(integer_width(&value.data_type()).is_some()
            && integer_width(cast.cast_type()).is_some())
    {
        return None;
    }
    value.cast_to(cast.cast_type()).ok()
}

fn predicate_value(value: &ScalarValue, data_type: &DataType) -> Option<PredicateValue> {
    let integer = match value {
        ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(*v),
        _ => None,
    };
    Some(match data_type {
        DataType::Int8 => PredicateValue::Int8(Some(integer?.try_into().ok()?)),
        DataType::Int16 => PredicateValue::Int16(Some(integer?.try_into().ok()?)),
        DataType::Int32 => PredicateValue::Int32(Some(integer?.try_into().ok()?)),
        DataType::Int64 => PredicateValue::Int64(Some(integer?)),
        DataType::Boolean => match value {
            ScalarValue::Boolean(Some(v)) => PredicateValue::Boolean(Some(*v)),
            _ => return None,
        },
        DataType::Utf8 => match value {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::Utf8View(Some(v))
            | ScalarValue::LargeUtf8(Some(v)) => PredicateValue::Utf8(Some(v.clone())),
            _ => return None,
        },
        _ => return None,
    })
}

fn comparison_op(op: Operator) -> Option<ComparisonOp> {
    Some(match op {
        Operator::Eq => ComparisonOp::Equal,
        Operator::NotEq => ComparisonOp::NotEqual,
        Operator::Lt => ComparisonOp::LessThan,
        Operator::LtEq => ComparisonOp::LessThanOrEqual,
        Operator::Gt => ComparisonOp::GreaterThan,
        Operator::GtEq => ComparisonOp::GreaterThanOrEqual,
        _ => return None,
    })
}

fn reverse(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::LessThan => ComparisonOp::GreaterThan,
        ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThanOrEqual,
        ComparisonOp::GreaterThan => ComparisonOp::LessThan,
        ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThanOrEqual,
        op => op,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_file_format::case_insensitive_adapter::CaseInsensitivePhysicalExprAdapterFactory;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::{Result, ToDFSchema};
    use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
    use datafusion::prelude::SessionContext;

    #[test]
    fn conversion_preserves_pruning_semantics() -> Result<()> {
        let logical = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("missing", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
        ]));
        let physical = Arc::new(Schema::new(vec![
            Field::new("Text", DataType::Utf8, true),
            Field::new("Id", DataType::Int32, true),
            Field::new("Flag", DataType::Boolean, true),
            Field::new("F", DataType::Float64, true),
        ]));
        let table = Schema::new(
            logical
                .fields()
                .iter()
                .cloned()
                .chain([Arc::new(Field::new("dt", DataType::Utf8, true))])
                .collect::<Vec<_>>(),
        )
        .to_dfschema()?;
        let state = SessionContext::new().state();
        let adapter =
            CaseInsensitivePhysicalExprAdapterFactory.create(logical.clone(), physical.clone())?;
        let convert = |sql: &str| -> Result<Option<Predicate>> {
            let expr = state.create_logical_expr(sql, &table)?;
            let expr = state.create_physical_expr(expr, &table)?;
            Ok(to_orc_predicate(
                &expr,
                &logical,
                &physical,
                adapter.as_ref(),
            ))
        };
        let int = PredicateValue::Int32(Some(12));
        for (sql, op) in [
            ("id = 12", ComparisonOp::Equal),
            ("id != 12", ComparisonOp::NotEqual),
            ("id < 12", ComparisonOp::LessThan),
            ("id <= 12", ComparisonOp::LessThanOrEqual),
            ("id > 12", ComparisonOp::GreaterThan),
            ("id >= 12", ComparisonOp::GreaterThanOrEqual),
            ("12 < id", ComparisonOp::GreaterThan),
            ("12 >= id", ComparisonOp::LessThanOrEqual),
            ("NOT (id = 12)", ComparisonOp::NotEqual),
        ] {
            assert_eq!(
                convert(sql)?,
                Some(Predicate::comparison("Id", op, int.clone())),
                "{sql}"
            );
        }
        let id = Predicate::eq("Id", int);
        let text = Predicate::eq("Text", PredicateValue::Utf8(Some("目标".into())));
        assert_eq!(convert("id = 12 AND length(text) > 1")?, Some(id.clone()));
        assert_eq!(
            convert("id = 12 AND text = '目标'")?,
            Some(Predicate::and(vec![id.clone(), text.clone()]))
        );
        assert_eq!(
            convert("id = 12 OR text = '目标'")?,
            Some(Predicate::or(vec![id.clone(), text]))
        );
        assert_eq!(convert("id = 12 AND dt = 'a'")?, Some(id.clone()));
        assert_eq!(
            convert("id IN (12, 13, NULL)")?,
            Some(Predicate::or(vec![
                id,
                Predicate::eq("Id", PredicateValue::Int32(Some(13)))
            ]))
        );
        assert_eq!(convert("text IS NULL")?, Some(Predicate::is_null("Text")));
        assert_eq!(
            convert("NOT (flag IS NULL)")?,
            Some(Predicate::is_not_null("Flag"))
        );
        assert_eq!(
            convert("flag = true")?,
            Some(Predicate::eq("Flag", PredicateValue::Boolean(Some(true))))
        );
        assert_eq!(
            convert("flag")?,
            Some(Predicate::eq("Flag", PredicateValue::Boolean(Some(true))))
        );
        assert_eq!(
            convert("NOT flag")?,
            Some(Predicate::ne("Flag", PredicateValue::Boolean(Some(true))))
        );
        for sql in [
            "id = 12 OR length(text) > 1",
            "NOT (id = 12 AND length(text) > 1)",
            "NOT (id = 12 OR text = '目标')",
            "id NOT IN (12, 13)",
            "id IN (12, missing)",
            "missing IS NULL",
            "dt IS NULL",
            "id = 2147483648",
            "CAST(id AS SMALLINT) = 12",
            "id = missing",
            "id + 1 = 12",
            "f != 1.0000000001",
            "CAST(id AS VARCHAR) = '12'",
        ] {
            assert_eq!(convert(sql)?, None, "{sql}");
        }
        Ok(())
    }
}
