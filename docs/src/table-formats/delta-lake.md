---
icon: lucide/triangle
---

# Delta Lake

!!! warning "Experimental Support"

An HMS or Glue table is treated as Delta Lake when its properties contain
`spark.sql.sources.provider` with value `DELTA`, case-insensitively. Lakelet
loads the table from its table location and reads the Delta transaction log
through `delta-rs`.

## Data Types

The same as `delta-rs`.

## Supported Operations

| Operation | Notes |
| --- | --- |
| `SELECT` | |
| [`DESCRIBE`](../sql/describe.md) | |
| [`SHOW CREATE TABLE`](../sql/show-create-table.md) | |
| [`INSERT INTO`](../sql/insert.md) | Appends rows to an existing table. |
| [`INSERT OVERWRITE`](../sql/insert.md) | Replaces all rows in an existing table. |

## Metadata Table

Delta Lake metadata tables are not currently exposed by Lakelet.
