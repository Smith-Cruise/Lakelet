use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Resolves table columns against data file columns ignoring ASCII case.
///
/// Hive metastores store lowercase column names, while engines such as Spark
/// keep the original case in the files they write. Matching by exact name
/// would read those columns as NULL. Exact matches still win, and a column is
/// only resolved case-insensitively when exactly one file column matches.
#[derive(Debug, Default)]
pub struct CaseInsensitivePhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for CaseInsensitivePhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let mut renames = HashMap::new();
        for logical_field in logical_file_schema.fields() {
            if physical_file_schema
                .field_with_name(logical_field.name())
                .is_ok()
            {
                continue;
            }
            let mut matches = physical_file_schema
                .fields()
                .iter()
                .filter(|field| field.name().eq_ignore_ascii_case(logical_field.name()));
            if let (Some(physical_field), None) = (matches.next(), matches.next()) {
                renames.insert(logical_field.name().clone(), physical_field.name().clone());
            }
        }
        if renames.is_empty() {
            return DefaultPhysicalExprAdapterFactory
                .create(logical_file_schema, physical_file_schema);
        }

        // Rename the logical fields to their physical spelling, so the default
        // adapter resolves them and rewritten columns carry the physical name
        // that statistics pruning and row filters look up.
        let renamed_logical_schema = Schema::new_with_metadata(
            logical_file_schema
                .fields()
                .iter()
                .map(|field| match renames.get(field.name()) {
                    Some(name) => Arc::new(field.as_ref().clone().with_name(name)),
                    None => Arc::clone(field),
                })
                .collect::<Vec<_>>(),
            logical_file_schema.metadata().clone(),
        );
        let inner = DefaultPhysicalExprAdapterFactory
            .create(Arc::new(renamed_logical_schema), physical_file_schema)?;
        Ok(Arc::new(CaseInsensitivePhysicalExprAdapter {
            renames,
            inner,
        }))
    }
}

#[derive(Debug)]
struct CaseInsensitivePhysicalExprAdapter {
    /// Logical column name to the physical column name it resolves to.
    renames: HashMap<String, String>,
    inner: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for CaseInsensitivePhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = expr
            .transform(|expr| {
                if let Some(column) = expr.downcast_ref::<Column>()
                    && let Some(name) = self.renames.get(column.name())
                {
                    return Ok(Transformed::yes(
                        Arc::new(Column::new(name, column.index())) as Arc<dyn PhysicalExpr>,
                    ));
                }
                Ok(Transformed::no(expr))
            })
            .data()?;
        self.inner.rewrite(expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};

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
}
