---
icon: lucide/hexagon
---

# Hive

An HMS or Glue table is treated as Hive when its properties match none of the
markers used by the other table formats — Hive is the fallback format. Lakelet
reads the schema from the metastore and lists data files from the table and
partition locations.

## Supported Data File Formats

| Data file format | SerDe
| --- | --- |
| TextFile | `LazySimpleSerDe`
| Parquet | `ParquetHiveSerDe`
| ORC | `OrcSerde`
| Avro | `AvroSerDe`

### TextFile

These table and SerDe properties are honored:

| Property |  Ddefault | Behavior |
| --- |---| --- |
| `field.delim`, `serialization.format` | `\001`| Field delimiter |
| `serialization.null.format` | `\N`  | Text that reads as `NULL` |
| `skip.header.line.count` | 0  | `0` or `1` |

A value that cannot be parsed as its column type is read as `NULL`. 
Empty fields are also read as `NULL`.

Complex type columns (`array`, `map`, `struct` and `uniontype`) are not supported,
so read as `NULL`. The other types are read as usual.

### Parquet

`INT96` timestamps, as written by Hive, Impala and older Spark versions, are
read as microsecond timestamps without timezone.

### ORC

Hive ACID tables are not supported.

### Avro

Everything is good.

## Data Types

| Hive type | Arrow type |
| --- | --- |
| `tinyint` | `Int8` |
| `smallint` | `Int16` |
| `int` | `Int32` |
| `bigint` | `Int64` |
| `float` | `Float32` |
| `double` | `Float64` |
| `boolean` | `Boolean` |
| `string` | `Utf8` |
| `varchar(n)`, `char(n)` | `Utf8` |
| `binary` | `Binary` |
| `date` | `Date32` |
| `timestamp` | Microsecond timestamp without a timezone |
| `timestamp with local time zone` | Microsecond timestamp in UTC |
| `decimal(p,s)` | `Decimal128(p,s)` |
| `decimal(p)` | `Decimal128(p,0)` |
| `decimal` | `Decimal128(10,0)` |
| `void` | `Null` |
| `array<T>` | `List` |
| `map<K,V>` | `Map` |
| `struct<name:T,...>` | `Struct` |
| `uniontype<T,...>` | Sparse `Union` |

## Supported Operations

| Operation | Notes |
| --- | --- |
| `SELECT` | |
| [`DESCRIBE`](../sql/describe.md) | |
| [`SHOW CREATE TABLE`](../sql/show-create-table.md) | |

## Metadata Table

**data_files**

The `data_files` metadata table lists visible, non-empty data files:

```sql
SELECT * FROM `table_name$data_files`;
```

| Column | Description |
| --- | --- |
| `file_path` | Full path of the data file |
| `file_size` | Size of the data file in bytes |

**partitions**

The `partitions` metadata table returns metastore partitions:

```sql
SELECT * FROM `table_name$partitions`;
```

An unpartitioned table returns empty.

| Column | Description |
| --- | --- |
| `partition` | Partition values as a string, such as `dt=2026-01-01/country=CN` |
| `data_file_count` | Number of data files in the partition |
| `total_data_file_size` | Combined size in bytes of data files |
