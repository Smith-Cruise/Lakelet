use async_trait::async_trait;
use paimon::catalog::{Catalog, Database, Identifier};
use paimon::spec::{Schema, SchemaChange};
use paimon::table::Table;
use paimon::{Error, Result};
use std::collections::HashMap;

/// A read-only `paimon::catalog::Catalog` that serves a single already-loaded
/// [`Table`].
///
/// `paimon-datafusion` exposes its system tables (`t$snapshots`, `t$files`,
/// ...) only through `PaimonSchemaProvider`, which resolves the base table via
/// a catalog handle. Lakelet builds Paimon tables directly from their location
/// (HMS/Glue catalogs hold no Paimon catalog at all), so this adapter hands
/// the one table back to the schema provider. `list_partitions` keeps the
/// trait's default implementation, which scans the file system through
/// `get_table`.
#[derive(Debug)]
pub(crate) struct SingleTablePaimonCatalog {
    table: Table,
}

// TODO: When paimon-rust expose public about metadata table builder, we can remove this code
impl SingleTablePaimonCatalog {
    pub fn new(table: Table) -> Self {
        Self { table }
    }

    fn held_identifier(&self) -> &Identifier {
        self.table.identifier()
    }

    fn unsupported<T>(operation: &str) -> Result<T> {
        Err(Error::Unsupported {
            message: format!("SingleTablePaimonCatalog does not support {operation}"),
        })
    }
}

#[async_trait]
impl Catalog for SingleTablePaimonCatalog {
    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(vec![self.held_identifier().database().to_string()])
    }

    async fn create_database(
        &self,
        _name: &str,
        _ignore_if_exists: bool,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Self::unsupported("create_database")
    }

    async fn get_database(&self, name: &str) -> Result<Database> {
        if name == self.held_identifier().database() {
            Ok(Database::new(name.to_string(), HashMap::new(), None))
        } else {
            Err(Error::DatabaseNotExist {
                database: name.to_string(),
            })
        }
    }

    async fn drop_database(
        &self,
        _name: &str,
        _ignore_if_not_exists: bool,
        _cascade: bool,
    ) -> Result<()> {
        Self::unsupported("drop_database")
    }

    async fn get_table(&self, identifier: &Identifier) -> Result<Table> {
        let held = self.held_identifier();
        if identifier.database() == held.database() && identifier.object() == held.object() {
            Ok(self.table.clone())
        } else {
            Err(Error::TableNotExist {
                full_name: identifier.full_name(),
            })
        }
    }

    async fn list_tables(&self, database_name: &str) -> Result<Vec<String>> {
        if database_name == self.held_identifier().database() {
            Ok(vec![self.held_identifier().object().to_string()])
        } else {
            Err(Error::DatabaseNotExist {
                database: database_name.to_string(),
            })
        }
    }

    async fn create_table(
        &self,
        _identifier: &Identifier,
        _creation: Schema,
        _ignore_if_exists: bool,
    ) -> Result<()> {
        Self::unsupported("create_table")
    }

    async fn drop_table(
        &self,
        _identifier: &Identifier,
        _ignore_if_not_exists: bool,
    ) -> Result<()> {
        Self::unsupported("drop_table")
    }

    async fn rename_table(
        &self,
        _from: &Identifier,
        _to: &Identifier,
        _ignore_if_not_exists: bool,
    ) -> Result<()> {
        Self::unsupported("rename_table")
    }

    async fn alter_table(
        &self,
        _identifier: &Identifier,
        _changes: Vec<SchemaChange>,
        _ignore_if_not_exists: bool,
    ) -> Result<()> {
        Self::unsupported("alter_table")
    }
}
