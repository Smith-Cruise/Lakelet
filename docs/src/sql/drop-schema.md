# DROP SCHEMA

## Synopsis

```text
DROP SCHEMA [ IF EXISTS ] [ catalog. ]schema_name [ CASCADE | RESTRICT ]
```

## Description

Drop a schema from the current catalog, or use `catalog.schema` to select
another catalog. By default, the schema must be empty. `RESTRICT` makes that
requirement explicit. `CASCADE` drops the schema and its tables.

`IF EXISTS` skips a missing schema. An existing, nonempty schema still
requires `CASCADE`.

## Supported Catalogs

- [Paimon FileSystem](../catalogs/paimon-filesystem.md) — `CASCADE` also deletes the data in those tables.

## Examples

This command removes `scratch` only if it exists and is empty.

```sql
DROP SCHEMA IF EXISTS paimon_local.scratch RESTRICT;
```
