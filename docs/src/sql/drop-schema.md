# DROP SCHEMA

## Synopsis

```text
DROP SCHEMA [ IF EXISTS ] [ catalog. ]schema_name [ CASCADE | RESTRICT ]
```

## Description

Remove a schema from a [Paimon FileSystem catalog](../catalogs/paimon-filesystem.md).
An unqualified name uses the current catalog. By default, the schema must be
empty. `RESTRICT` makes that requirement explicit. `CASCADE` removes the
schema and all tables and data in it.

`IF EXISTS` skips a missing schema. An existing, nonempty schema still
requires `CASCADE`.

## Examples

This command removes `scratch` only if it exists and is empty.

```sql
DROP SCHEMA IF EXISTS paimon_local.scratch RESTRICT;
```
