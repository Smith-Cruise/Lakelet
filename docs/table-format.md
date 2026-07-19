# Table Format

Lakelet determines a table format from metastore table properties. Table
formats are discovered through HMS or Glue.

## Hive

### Supported File Formats

| Input format | Status
| --- | --- |
| TextFile | Supported
| Parquet | Supported
| ORC | Not supported

### Data Types

| Hive type | Arrow type |
| --- | --- |
| `tinyint` | `Int8` |
| `smallint` | `Int16` |
| `int`, `integer` | `Int32` |
| `bigint`, `long` | `Int64` |
| `float` | `Float32` |
| `double`, `double precision` | `Float64` |
| `boolean` | `Boolean` |
| `string`, `binary string` | `Utf8` |
| `varchar(...)`, `char(...)` | `Utf8` |
| `binary` | `Binary` |
| `date` | `Date32` |
| `timestamp` | Microsecond timestamp without a timezone |
| `decimal(p,s)` | `Decimal128(p,s)` |
| `decimal` or an unparseable decimal declaration | `Decimal128(38,10)` |

Types not listed above, including Hive complex types, are not currently
supported.

### Metadata Table

**data_files**

The `data_files` metadata table lists visible, non-empty data files:

```sql
SELECT * FROM `table_name$data_files`;
```

It returns `file_path` and `file_size`. Files whose names start with `_` or `.`
and zero-byte files are excluded.

**partitions**

The `partitions` metadata table returns one row for each metastore partition:

```sql
SELECT * FROM `table_name$partitions`;
```

It returns `partition`, `data_file_count`, and `total_data_file_size`.
`partition` is a string such as `dt=2026-01-01/country=CN`.
An unpartitioned table returns no rows.

## Iceberg

An HMS or Glue table is treated as Iceberg when its properties contain
`metadata_location`. Lakelet loads the table directly from that metadata file.

Only Parquet data files are currently supported.

### Data Types

The same as `iceberg-rust`.

### Metadata Table

**snapshots**

```sql
SELECT * FROM `table_name$snapshots`;
```

**manifests**

```sql
SELECT * FROM `table_name$manifests`;
```

## Delta Lake

An HMS or Glue table is treated as Delta Lake when its properties contain
`spark.sql.sources.provider` with value `DELTA`, case-insensitively. Lakelet
loads the table from its table location and reads the Delta transaction log
through `delta-rs`.

Delta Lake support is not yet complete and should be treated as experimental.

### Data Types

The same as `delta-rs`.

### Metadata Table

Delta Lake metadata tables are not currently exposed by Lakelet.

## Paimon

An HMS or Glue table is treated as Paimon when its properties contain
`table_type` with value `PAIMON`, case-insensitively. Lakelet loads the latest
Paimon schema from the table location and reads the table through
`paimon-datafusion`.

Only Parquet data files are currently supported.

### Data Types

The same as `paimon-rust`.

### Metadata Table

Paimon metadata tables are not currently exposed by Lakelet.
