# DESCRIBE

## Synopsis

```text
DESCRIBE table_name
DESC table_name
```

## Description

List the columns of a table in definition order. `DESC` is a short form of
`DESCRIBE`. Each row describes one column.

| Result column | Meaning |
| --- | --- |
| `column_name` | Name of the column. |
| `sql_type` | SQL type, such as `INT`. |
| `data_type` | Underlying Arrow type, such as `Int32`. |
| `is_nullable` | `YES` if the column allows nulls, otherwise `NO`. |

## Examples

The [TPC-H catalog](../catalogs/tpch.md) includes a `nation` table.

```sql
DESC tpch.sf1.nation;
```
