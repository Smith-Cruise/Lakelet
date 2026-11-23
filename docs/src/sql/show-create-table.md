# SHOW CREATE TABLE

## Synopsis

```text
SHOW CREATE TABLE table_name
```

## Description

Return one row with a `definition` column containing the table's SQL
definition. Use this to inspect columns, partitioning, storage location and
table options where available.

Lakelet exposes definitions for Hive, Iceberg, Delta Lake and Paimon data
tables. Metadata tables and generated TPC-H and TPC-DS tables have no
definition available through this command. To inspect their columns, use
[DESCRIBE](describe.md).

## Examples

Inspect the [example Paimon table](create-table.md).

```sql
SHOW CREATE TABLE paimon_local.sales.orders;
```
