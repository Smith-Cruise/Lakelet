---
icon: lucide/triangle
---

# Delta Lake

An HMS or Glue table is treated as Delta Lake when its properties contain
`spark.sql.sources.provider` with value `DELTA`, case-insensitively. Lakelet
loads the table from its table location and reads the Delta transaction log
through `delta-rs`.

Delta Lake support is not yet complete and should be treated as experimental.

## Data Types

The same as `delta-rs`.

## Metadata Table

Delta Lake metadata tables are not currently exposed by Lakelet.
