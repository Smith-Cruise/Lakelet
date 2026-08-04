# Hive

## Supported Data File Formats

| Input format | Status
| --- | --- |
| TextFile | Supported
| Parquet | Supported
| ORC | Not supported

## Data Types

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

## Metadata Table

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
