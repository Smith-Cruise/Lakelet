# SHOW SCHEMAS

## Synopsis

```text
SHOW SCHEMAS [ FROM catalog ] [ LIKE 'pattern' ]
```

## Description

List schemas in the current catalog, or specify a catalog with `FROM`. The
result has one column, `schema_name`, sorted by name. An optional `LIKE`
pattern filters schema names.

## Examples

```sql
SHOW SCHEMAS;
```

Find [TPC-H](../catalogs/tpch.md) schemas whose names start with `sf1`.

```sql
SHOW SCHEMAS FROM tpch LIKE 'sf1%';
```
