# SHOW TABLES

## Synopsis

```text
SHOW TABLES [ FROM [ catalog. ]schema ] [ LIKE 'pattern' ]
```

## Description

List tables in the current schema. Use `FROM schema` to select a schema in
the current catalog, or `FROM catalog.schema` to name both. The result has
one column, `table_name`, sorted by name. `LIKE` filters table names within
the selected schema.

## Examples

```sql
SHOW TABLES;
```

Find tables starting with `ord` in the `sf1` schema of the
[TPC-H catalog](../catalogs/tpch.md).

```sql
SHOW TABLES FROM tpch.sf1 LIKE 'ord%';
```
