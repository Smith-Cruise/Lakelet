# CREATE TABLE

## Synopsis

```text
CREATE TABLE [ IF NOT EXISTS ] table_name (
    column_definition [, column_definition ...]
    [, PRIMARY KEY (column_name [, column_name ...]) ]
)
[ PARTITIONED BY (column_name [, column_name ...]) ]
[ USING PAIMON ]
[ WITH ('key' = 'value' [, 'key' = 'value' ...]) ]

CREATE TABLE [ IF NOT EXISTS ] table_name
[ PARTITIONED BY (column_name [, column_name ...]) ]
[ USING PAIMON ]
[ WITH ('key' = 'value' [, 'key' = 'value' ...]) ]
AS query

CREATE TABLE [ IF NOT EXISTS ] table_name LIKE source_table
[ PARTITIONED BY (column_name [, column_name ...]) ]
[ USING PAIMON ]
[ WITH ('key' = 'value' [, 'key' = 'value' ...]) ]
```

Each column definition names a column and its type.

```text
column_name data_type [ NULL | NOT NULL ] [ COMMENT 'text' ] [ PRIMARY KEY ]
```

## Description

Create a Paimon table in an existing schema in a
[Paimon FileSystem catalog](../catalogs/paimon-filesystem.md).
`USING PAIMON` is optional in this catalog. An unqualified table name uses the
current catalog and schema. If the table already exists, `IF NOT EXISTS`
keeps its definition and data.

You can declare one primary key, either on a single column or as a table constraint.
Primary-key columns are non-nullable. In `PARTITIONED BY`, use column names
from the table definition. In `WITH`, single-quote both keys and values.
See [Paimon table options](../table-formats/paimon.md#table-options) for the
available properties.

`AS query` derives the columns from the query and writes its result into the
new table. `LIKE source_table` creates an empty table with the source column
names, types and nullability. Specify any partitioning or table options for
the new table separately.

## Examples

This table in the [sales schema](create-schema.md) uses the order day as its
partition key.

```sql
CREATE TABLE paimon_local.sales.orders (
    id INT,
    order_day STRING,
    PRIMARY KEY (id, order_day)
)
PARTITIONED BY (order_day)
USING PAIMON
WITH ('bucket' = '2', 'file.format' = 'parquet');
```

Use `AS SELECT` to populate a table of order IDs and `LIKE` to make an empty
copy with the same columns.

```sql
CREATE TABLE paimon_local.sales.order_ids AS
SELECT id FROM paimon_local.sales.orders;

CREATE TABLE paimon_local.sales.order_ids_copy LIKE paimon_local.sales.order_ids;
```
