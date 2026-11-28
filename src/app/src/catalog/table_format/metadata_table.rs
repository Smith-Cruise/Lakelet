use datafusion::common::Result;
use datafusion::error::DataFusionError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTableType {
    DataFiles,
    Partitions,
    Snapshots,
    Manifests,
    History,                   // Iceberg only
    Options,                   // Paimon only
    Schemas,                   // Paimon only
    Tags,                      // Paimon only
    Branches,                  // Paimon only
    PaimonTableIndexes,        // Paimon only
    PaimonPhysicalFilesSize,   // Paimon only
    PaimonReferencedFilesSize, // Paimon only
    PaimonAggregationFields,   // Paimon only
    PaimonAuditLog,            // Paimon only
    PaimonBuckets,             // Paimon only
    PaimonConsumers,           // Paimon only
}

impl TryFrom<&str> for MetadataTableType {
    type Error = String;

    fn try_from(metadata_table_name: &str) -> Result<Self, Self::Error> {
        match metadata_table_name {
            "data_files" => Ok(MetadataTableType::DataFiles),
            "partitions" => Ok(MetadataTableType::Partitions),
            "snapshots" => Ok(MetadataTableType::Snapshots),
            "manifests" => Ok(MetadataTableType::Manifests),
            "history" => Ok(MetadataTableType::History),
            "options" => Ok(MetadataTableType::Options),
            "schemas" => Ok(MetadataTableType::Schemas),
            "tags" => Ok(MetadataTableType::Tags),
            "branches" => Ok(MetadataTableType::Branches),
            "table_indexes" => Ok(MetadataTableType::PaimonTableIndexes),
            "physical_files_size" => Ok(MetadataTableType::PaimonPhysicalFilesSize),
            "referenced_files_size" => Ok(MetadataTableType::PaimonReferencedFilesSize),
            "aggregation_fields" => Ok(MetadataTableType::PaimonAggregationFields),
            "audit_log" => Ok(MetadataTableType::PaimonAuditLog),
            "buckets" => Ok(MetadataTableType::PaimonBuckets),
            "consumers" => Ok(MetadataTableType::PaimonConsumers),
            _ => Err(format!(
                "invalid metadata table type: {metadata_table_name}"
            )),
        }
    }
}

pub fn parse_table_reference(tbl_name: &str) -> Result<(String, Option<MetadataTableType>)> {
    let (table_name, metadata_table_name) = match tbl_name.split_once("$") {
        Some((table_name, metadata_table_name)) => (table_name, Some(metadata_table_name)),
        None => (tbl_name, None),
    };

    if table_name.is_empty() {
        return Err(DataFusionError::Plan("table name can't be empty".into()));
    }

    let metadata_table_type = metadata_table_name
        .map(MetadataTableType::try_from)
        .transpose()
        .map_err(DataFusionError::Plan)?;

    Ok((table_name.to_string(), metadata_table_type))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ExtendedParser;
    use crate::sql::statements::ExtendedStatement;
    use sqlparser::ast::{ObjectNamePart, SetExpr, Statement, TableFactor};

    /// Parse a `SELECT` and resolve its single table reference the same way the
    /// catalog providers do, so we exercise the full path from SQL text to
    /// `(table_name, MetadataTableType)`.
    fn resolve_table_reference(sql: &str) -> (String, Option<MetadataTableType>) {
        let statements = ExtendedParser::parse_sql(sql).expect("sql should parse");
        let ExtendedStatement::SQLStatement(stmt) = &statements[0];
        let Statement::Query(query) = stmt.as_ref() else {
            panic!("expected a query");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a select");
        };
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            panic!("expected a table factor");
        };
        let table_ident = match name.0.last().expect("table name has a part") {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
            other => panic!("unexpected object name part: {other:?}"),
        };
        parse_table_reference(&table_ident).expect("table reference should resolve")
    }

    #[test]
    fn parse_metadata_table() {
        // Every metadata table suffix, in MetadataTableType declaration order.
        // The unquoted `$` suffix is the case that used to fail before the
        // dialect change.
        let cases = [
            ("data_files", MetadataTableType::DataFiles),
            ("partitions", MetadataTableType::Partitions),
            ("snapshots", MetadataTableType::Snapshots),
            ("manifests", MetadataTableType::Manifests),
            ("history", MetadataTableType::History),
            ("options", MetadataTableType::Options),
            ("schemas", MetadataTableType::Schemas),
            ("tags", MetadataTableType::Tags),
            ("branches", MetadataTableType::Branches),
            ("table_indexes", MetadataTableType::PaimonTableIndexes),
            (
                "physical_files_size",
                MetadataTableType::PaimonPhysicalFilesSize,
            ),
            (
                "referenced_files_size",
                MetadataTableType::PaimonReferencedFilesSize,
            ),
            (
                "aggregation_fields",
                MetadataTableType::PaimonAggregationFields,
            ),
            ("audit_log", MetadataTableType::PaimonAuditLog),
            ("buckets", MetadataTableType::PaimonBuckets),
            ("consumers", MetadataTableType::PaimonConsumers),
        ];
        for (suffix, expected) in cases {
            assert_eq!(
                resolve_table_reference(&format!("select * from orders${suffix}")),
                ("orders".to_string(), Some(expected)),
                "suffix: {suffix}"
            );
            assert_eq!(
                resolve_table_reference(&format!("select * from `orders${suffix}`")),
                ("orders".to_string(), Some(expected)),
                "quoted suffix: {suffix}"
            );
        }

        // A plain table without a `$` suffix carries no metadata type.
        assert_eq!(
            resolve_table_reference("select * from orders"),
            ("orders".to_string(), None)
        );
    }
}
