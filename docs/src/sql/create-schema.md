# CREATE SCHEMA

## Synopsis

```text
CREATE SCHEMA [ IF NOT EXISTS ] [ catalog. ]schema_name
```

## Description

Create a schema in a [Paimon FileSystem catalog](../catalogs/paimon-filesystem.md).
A schema name on its own uses the current catalog. If the schema already
exists, the statement fails unless you specify `IF NOT EXISTS`.

## Examples

```sql
CREATE SCHEMA IF NOT EXISTS paimon_local.sales;
```

After selecting the catalog with [USE](use.md), the schema name alone is enough.

```sql
USE CATALOG paimon_local;
CREATE SCHEMA IF NOT EXISTS sales;
```
