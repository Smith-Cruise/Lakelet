# DROP TABLE

## Synopsis

```text
DROP TABLE [ IF EXISTS ] table_name
```

## Description

Drop a table. Use `IF EXISTS` to skip a missing table.

Use `table` for a table in the current schema, `schema.table` for another
schema in the current catalog, or `catalog.schema.table` for a fully
qualified name.

## Supported Catalogs

- [Paimon FileSystem](../catalogs/paimon-filesystem.md) — Dropping a table deletes its warehouse directory, including its data and metadata.

## Examples

```sql
DROP TABLE IF EXISTS paimon_local.sales.order_ids_copy;
```
