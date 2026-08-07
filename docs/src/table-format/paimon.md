---
icon: lucide/layers
---

# Paimon

An HMS or Glue table is treated as Paimon when its properties contain
`table_type` with value `PAIMON`, case-insensitively. Tables from a
[Paimon filesystem catalog](../catalog.md#paimon-filesystem) are always
Paimon. Lakelet loads the latest Paimon schema from the table location and
reads the table through `paimon-datafusion`.

Only Parquet data files are currently supported.

## Data Types

The same as `paimon-rust`.

## Metadata Table

Paimon metadata tables are not currently exposed by Lakelet.
