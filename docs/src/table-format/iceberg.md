# Iceberg

An HMS or Glue table is treated as Iceberg when its properties contain
`metadata_location`. Lakelet loads the table directly from that metadata file.

Only Parquet data files are currently supported.

## Data Types

The same as `iceberg-rust`.

## Metadata Table

**snapshots**

```sql
SELECT * FROM `table_name$snapshots`;
```

**manifests**

```sql
SELECT * FROM `table_name$manifests`;
```
