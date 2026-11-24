# CREATE SCHEMA

## Synopsis

```text
CREATE SCHEMA [ IF NOT EXISTS ] [ catalog. ]schema_name
```

## Description

Create a schema in the current catalog, or use `catalog.schema` to select
another catalog. If the schema already exists, the statement fails unless
you specify `IF NOT EXISTS`.

## Supported Catalogs

- [Paimon FileSystem](../catalogs/paimon-filesystem.md)

## Examples

```sql
CREATE SCHEMA IF NOT EXISTS paimon_local.sales;
```

After selecting the catalog with [USE](use.md), the schema name alone is enough.

```sql
USE CATALOG paimon_local;
CREATE SCHEMA IF NOT EXISTS sales;
```
