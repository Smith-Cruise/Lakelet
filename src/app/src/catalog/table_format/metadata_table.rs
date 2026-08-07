#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTableType {
    DataFiles,
    Partitions,
    Snapshots,
    Manifests,
    History,                   // Iceberg
    Options,                   // Paimon only
    Schemas,                   // Paimon only
    Tags,                      // Paimon only
    Branches,                  // Paimon only
    PaimonTableIndexes,        // Paimon only
    PaimonPhysicalFilesSize,   // Paimon only
    PaimonReferencedFilesSize, // Paimon only
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
            _ => Err(format!(
                "invalid metadata table type: {metadata_table_name}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table_format::table_provider_factory::parse_table_reference;
    use crate::sql::parser::ExtendedParser;
    use crate::sql::statements::ExtendedStatement;
    use sqlparser::ast::{ObjectNamePart, SetExpr, Statement, TableFactor};

    /// Parse a `SELECT` and resolve its single table reference the same way the
    /// catalog providers do, so we exercise the full path from SQL text to
    /// `(table_name, MetadataTableType)`.
    fn resolve_table_reference(sql: &str) -> (String, Option<MetadataTableType>) {
        let statements = ExtendedParser::parse_sql(sql).expect("sql should parse");
        let ExtendedStatement::SQLStatement(stmt) = &statements[0] else {
            panic!("expected a SQL statement");
        };
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
        ];
        for (suffix, expected) in cases {
            assert_eq!(
                resolve_table_reference(&format!("select * from orders${suffix}")),
                ("orders".to_string(), Some(expected)),
                "suffix: {suffix}"
            );
        }

        // Backtick-quoted form resolves to the exact same result.
        assert_eq!(
            resolve_table_reference("select * from `orders$data_files`"),
            ("orders".to_string(), Some(MetadataTableType::DataFiles))
        );

        // Qualified name `db.table$type`, with and without quoting the table part.
        assert_eq!(
            resolve_table_reference("select * from sales.orders$snapshots"),
            ("orders".to_string(), Some(MetadataTableType::Snapshots))
        );
        assert_eq!(
            resolve_table_reference("select * from sales.`orders$snapshots`"),
            ("orders".to_string(), Some(MetadataTableType::Snapshots))
        );

        // A plain table without a `$` suffix carries no metadata type.
        assert_eq!(
            resolve_table_reference("select * from orders"),
            ("orders".to_string(), None)
        );
    }
}
