# USE

## Synopsis

```text
USE CATALOG catalog
USE schema
USE catalog.schema
```

## Description

Set the defaults Lakelet uses to resolve names in the current session.

| Form | Effect |
| --- | --- |
| `USE CATALOG catalog` | Switch to the catalog and keep the current schema name. |
| `USE schema` | Select a schema in the current catalog. |
| `USE catalog.schema` | Select both the catalog and the schema. |

In the interactive CLI, subsequent statements use these defaults.
[Flight SQL](../getting-started.md#flight-sql) starts a fresh session for each
query. Use a fully qualified table name or pass the catalog and schema in the
request headers when you need the same defaults across queries.

## Examples

With the [TPC-H catalog](../catalogs/tpch.md) configured, select its scale
factor 1 schema and query a table by name.

```sql
USE tpch.sf1;
SELECT count(*) FROM nation;
```

You can also choose the catalog and schema separately in the same session.

```sql
USE CATALOG tpch;
USE sf1;
```
