# CALL PROCEDURE

## Synopsis

```text
CALL table_format.procedure_name(table => 'table_name' [, argument_name => value] ...)
```

## Description

Run a procedure for a table format. Pass the table name as a single-quoted
string using the named argument `table =>`. Use `'catalog.schema.table'` to
identify the table explicitly. Shorter names use the current catalog and schema.

## Supported Catalogs

- [Paimon FileSystem](../catalogs/paimon-filesystem.md)

## Examples

After [inserting rows](insert.md) into `paimon_local.sales.orders`, you can
tag the latest snapshot for later reference.

```sql
CALL paimon.create_tag(table => 'paimon_local.sales.orders', tag => 'daily_close');
```

To remove that tag, run the `delete_tag` procedure with the same table and tag name.

```sql
CALL paimon.delete_tag(table => 'paimon_local.sales.orders', tag => 'daily_close');
```
