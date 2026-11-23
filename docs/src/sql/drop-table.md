# DROP TABLE

## Synopsis

```text
DROP TABLE [ IF EXISTS ] table_name
```

## Description

Drop a table in a [Paimon FileSystem catalog](../catalogs/paimon-filesystem.md).
This removes the table's directory from the warehouse, including its data and
metadata. Use `IF EXISTS` to skip a missing table.

Use `table` for a table in the current schema, `schema.table` for another
schema in the current catalog, or `catalog.schema.table` for a fully
qualified name.

## Examples

```sql
DROP TABLE IF EXISTS paimon_local.sales.order_ids_copy;
```
