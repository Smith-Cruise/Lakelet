use super::ExtendedSessionContext;
use crate::catalog::CallProcedureRequest;
use crate::catalog::table_format::TableFormat;
use crate::parser::ExtendedParser;
use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments, Value,
};
use datafusion::sql::planner::IdentNormalizer;

impl ExtendedSessionContext {
    pub(super) async fn handle_call_stmt(&self, function: &Function) -> Result<DataFrame> {
        let state = self.session_context.state();
        let options = state.config_options();
        let normalizer = IdentNormalizer::new(options.sql_parser.enable_ident_normalization);
        let parts = function
            .name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .cloned()
                    .map(|ident| normalizer.normalize(ident))
                    .ok_or_else(|| plan_datafusion_err!("CALL requires identifier names"))
            })
            .collect::<Result<Vec<_>>>()?;
        let [table_format, procedure_name] = parts.as_slice() else {
            return plan_err!("Expected CALL table_format.procedure_name(...)");
        };
        let table_format: TableFormat = table_format.parse()?;
        let mut table = None;
        if let FunctionArguments::List(args) = &function.args {
            for arg in &args.args {
                if let FunctionArg::Named {
                    name,
                    arg,
                    operator: FunctionArgOperator::RightArrow,
                } = arg
                    && name.value.eq_ignore_ascii_case("table")
                {
                    if table.is_some() {
                        return plan_err!("Procedure argument table is duplicated.");
                    }
                    let FunctionArgExpr::Expr(Expr::Value(value)) = arg else {
                        return plan_err!("Procedure argument table must be a string literal");
                    };
                    let Value::SingleQuotedString(value) = &value.value else {
                        return plan_err!("Procedure argument table must be a string literal");
                    };
                    let table_name = ExtendedParser::parse_table_name(value)?;
                    table = Some(self.resolve_ddl_table_name(&table_name)?);
                }
            }
        }
        let table =
            table.ok_or_else(|| plan_datafusion_err!("Missing required argument: 'table'"))?;
        let catalog_name = table.catalog.as_ref();
        let provider = self
            .catalog_provider_list
            .get_catalog(catalog_name)
            .ok_or_else(|| plan_datafusion_err!("unknown catalog {catalog_name}"))?;
        provider
            .call_procedure(
                &self.session_context,
                &CallProcedureRequest {
                    table_format,
                    table_name: table,
                    procedure_name: procedure_name.clone(),
                    procedure_args: &function.args,
                },
            )
            .await
    }
}
