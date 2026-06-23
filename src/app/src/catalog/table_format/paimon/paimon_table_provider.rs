use crate::catalog::TableDefinitionBuilder;
use crate::table_format::TableFormat;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, TableReference};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use paimon_datafusion::PaimonTableProvider;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct DobbyDbPaimonTableProvider {
    inner_provider: PaimonTableProvider,
    table_definition: String,
}

impl DobbyDbPaimonTableProvider {
    pub fn try_new(
        table_reference: TableReference,
        table_location: String,
        inner_provider: PaimonTableProvider,
    ) -> Result<Self> {
        let table = inner_provider.table();
        let partition_column_names = table.schema().partition_keys().to_vec();
        let table_definition = TableDefinitionBuilder::new(
            table_reference,
            table_location,
            TableFormat::Paimon,
            inner_provider.schema().as_ref().clone(),
        )
        .with_partition_column_names(partition_column_names)
        .build()?;
        Ok(Self {
            inner_provider,
            table_definition,
        })
    }
}

#[async_trait]
impl TableProvider for DobbyDbPaimonTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner_provider.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner_provider.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(&self.table_definition)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner_provider.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner_provider.supports_filters_pushdown(filters)
    }
}
