# CREATE TABLE

## Synopsis

```text
CREATE TABLE [ IF NOT EXISTS ] table_name (
    column_definition [, column_definition ...]
    [, PRIMARY KEY (column_name [, column_name ...]) ]
)
[ PARTITIONED BY (column_name [, column_name ...]) ]
[ USING table_format ]
[ WITH ('key' = 'value' [, 'key' = 'value' ...]) ]

CREATE TABLE [ IF NOT EXISTS ] table_name
[ PARTITIONED BY (column_name [, column_name ...]) ]
[ USING table_format ]
[ WITH ('key' = 'value' [, 'key' = 'value' ...]) ]
AS query

CREATE TABLE [ IF NOT EXISTS ] table_name LIKE source_table
[ PARTITIONED BY (column_name [, column_name ...]) ]
[ USING table_format ]
[ WITH ('key' = 'value' [, 'key' = 'value' ...]) ]
```

Each column definition names a column and its type.

```text
column_name data_type [ NULL | NOT NULL ] [ COMMENT 'text' ] [ PRIMARY KEY ]
```

## Description

Create a table in an existing schema. An unqualified table name uses the
current catalog and schema. If the table already exists, `IF NOT EXISTS`
keeps its definition and data.

`USING table_format` selects the format of the new table. You can omit
`USING` when the target catalog has a default table format. Use `WITH` to
set [table format options](#table-format-options). Single-quote both keys
and values, including numeric and Boolean values.

Columns are nullable by default. Declare a primary key on a single column or
as a table constraint for one or more columns. A table can have one primary
key. Its columns are non-nullable and cannot be declared `NULL`. In
`PARTITIONED BY`, list column names from the table definition.

`AS query` derives the columns from the query and writes its results into
the new table. `LIKE source_table` creates an empty table with the source
column names, types and nullability. Specify any partitioning and table
options for the new table separately.

## Supported catalogs

- [Paimon FileSystem](../catalogs/paimon-filesystem.md) — Creates tables under its configured warehouse and defaults to `USING PAIMON`.

## Examples

In the current catalog, create an `orders` table in the
[sales schema](create-schema.md) and partition it by order day.

```sql
CREATE TABLE sales.orders (
    id INT,
    order_day STRING,
    PRIMARY KEY (id, order_day)
)
PARTITIONED BY (order_day);
```

Use `AS SELECT` to populate a table of order IDs and `LIKE` to make an empty
copy with the same columns.

```sql
CREATE TABLE sales.order_ids AS
SELECT id FROM sales.orders;

CREATE TABLE sales.order_ids_copy LIKE sales.order_ids;
```

## Table format options

### Paimon

Use `WITH` to set Paimon table properties. This example creates a primary-key
table with two fixed buckets and Parquet data files.

```sql
CREATE TABLE paimon_local.sales.bucketed_orders (id INT PRIMARY KEY, order_day STRING)
USING PAIMON
WITH ('bucket' = '2', 'file.format' = 'parquet');
```

The following `WITH` properties are documented in the
[Paimon table options reference](https://paimon.apache.org/docs/rust/sql/#table-options).

#### Bucket Configuration

| Option | Description |
| --- | --- |
| `'bucket' = 'N'` | Fixed N buckets (e.g. 1, 2, 4) |
| `'bucket' = '-1'` | Dynamic bucket mode (HASH index) |
| `'bucket' = '-2'` | Postpone bucket mode (deferred assignment) |
| `'bucket-key' = 'col'` | Explicit bucket key column |
| `'bucket-function.type' = 'default' \| 'mod' \| 'hive'` | Function used to map fixed bucket keys to bucket ids |

#### Merge Engine

| Option | Description |
| --- | --- |
| `'merge-engine' = 'deduplicate'` | Deduplicate engine (default for PK tables), last write wins |
| `'merge-engine' = 'first-row'` | Keeps the first written row |
| `'merge-engine' = 'partial-update'` | Basic partial-update engine for PK tables |
| `'merge-engine' = 'aggregation'` | Basic aggregation engine for PK tables |

For primary-key tables, `partial-update` keeps the latest non-null value of each
field. Set `'ignore-delete' = 'true'` or
`'partial-update.ignore-delete' = 'true'` to ignore retract rows; otherwise they
are rejected. Once enabled, `ignore-delete` cannot be changed back to `false`.
Creating partial-update tables with sequence groups or aggregation options is
not supported.

The basic `aggregation` engine supports fixed buckets and dynamic buckets
(`'bucket' = '-1'`) when the primary key includes all partition columns. Configure
functions with `fields.<field>.aggregate-function` or
`fields.default-aggregate-function`. Supported functions are `sum`, `product`,
`min`, `max`, `last_value`, `first_value`, `last_non_null_value`,
`first_non_null_value`, `bool_and`, `bool_or`, and `listagg`. Sequence fields
always use `last_value` and cannot have an explicit aggregate function.
Retract rows, deletion-vector writes, cross-partition dynamic bucket writes,
and advanced aggregation options such as `ignore-retract`, `distinct`,
`nested-key`, `count-limit`, and sequence groups are not supported.

For deletion-vector-enabled primary-key tables using `deduplicate`, batch reads
hide uncompacted level-0 files by default. Set
`'deletion-vectors.merge-on-read' = 'true'` to include and merge them on read.
This option only takes effect when `'deletion-vectors.enabled' = 'true'` and
only affects batch snapshot reads. Deletion-vector writes and merge-on-read
are not supported for `partial-update` or `aggregation` tables.

#### Global Index Options

| Option | Default | Description |
| --- | --- | --- |
| `row-tracking.enabled` | `false` | Enables stable row ids required by global index files. |
| `data-evolution.enabled` | `false` | Enables row-id-aware table evolution and partial-column writes. |
| `global-index.enabled` | `true` | Enables global index metadata and global-index-aware reads. |
| `global-index.row-count-per-shard` | `100000` | Maximum row count per vector global-index shard. |
| `sorted-index.records-per-range` | `100000` | Maximum row count per BTree, bitmap, multivalue, or FM global-index file range; falls back to legacy `btree-index.records-per-range`. |
| `btree-index.block-size` | `64kb` | Target BTree data-block size. |
| `btree-index.file-version` | `1` | BTree file version to write: `1` or `2`. V2 uses adaptive postings and requires V2-capable readers. |
| `btree-index.bloom-filter.enabled` | `false` | Writes a Bloom filter used to avoid BTree data-block reads for missing equality and `IN` keys. |
| `btree-index.compression` | `none` | BTree block compression: `none`, `zstd`, `lz4`, or `lzo`. |
| `btree-index.compression-level` | `1` | BTree compression level (used by codecs that support levels). |
| `bitmap-index.dictionary-block-size` | `16kb` | Target bitmap dictionary-block size. |
| `bitmap-index.compression` | `none` | Bitmap dictionary/index block compression: `none`, `zstd`, `lz4`, or `lzo`. |
| `bitmap-index.compression-level` | `1` | Bitmap compression level (used by codecs that support levels). |
| `multivalue-index.dictionary-block-size` | `16kb` | Target multivalue dictionary-block size. |
| `multivalue-index.compression` | `none` | Multivalue dictionary/index block compression: `none`, `zstd`, `lz4`, or `lzo`. |
| `multivalue-index.compression-level` | `1` | Multivalue compression level (used by codecs that support levels). |
| `fm-index.partition-size` | `16mb` | Maximum encoded symbol count targeted by each FM partition. A single encoded value must be smaller than this limit. |
| `fm-index.partition-row-count` | `100000` | Maximum row count per FM partition. |
| `fm-index.sa-sample-rate` | `32` | Power-of-two suffix-array sampling rate used by FM locate operations. |
| `fm-index.compression` | `lz4` | FM block compression: `none`, `zstd`, `lz4`, or `lzo`. |
| `fm-index.compression-level` | `1` | FM compression level (used by codecs that support levels). |
| `fm-index.read-cache-size` | `64mb` | Scan-scoped decoded FM block cache size. |
| `fm-index.demand-page-size` | `512kb` | Target compressed-block read-ahead size for FM demand paging. |
| `fm-index.locate-cost-ratio` | `0.001` | Maximum estimated locate work as a fraction of indexed text; denser matches fall back to the source scan. |
| `btree-index.fallback-scan-max-size` | `256mb` | Maximum total size of BTree global-index files requiring fallback scans for range/between and suffix/contains/complex LIKE predicates; files proven to match completely by metadata need no scan and are excluded. `0` disables BTree fallback index scans. |
| `bitmap-index.fallback-scan-max-size` | `256mb` | Maximum total size of selected bitmap global-index files for fallback scans used by range/between and suffix/contains/complex LIKE predicates; `0` disables bitmap fallback index scans. |
| `global-index.search-mode` | `fast` | Global index coverage mode for reads: `fast`, `full`, or `detail`. |
| `global-index.thread-num` | `32` | Number of concurrent global-index search tasks; must be greater than 0 and must not exceed the runtime's task limit. This does not limit Vindex file range reads. |
| `global-index.vindex.read-thread-num` | `64` | Maximum number of concurrent Vindex file range reads shared by one search operation; must be greater than 0 and must not exceed the runtime semaphore limit. |
| `global-index.column-update-action` | `THROW_ERROR` | What a commit does when it updates an indexed column: `THROW_ERROR` rejects the commit, `DROP_PARTITION_INDEX` drops the affected partition index instead. |

Global index creation in Paimon requires row tracking and data evolution;
primary-key tables and tables with deletion vectors enabled are not supported.
Setting these options alone does not build an index.

`global-index.vindex.read-thread-num` is independent of `global-index.thread-num`.
Set both explicitly if index search tasks and Vindex file reads should use the
same concurrency limit.

#### Variant Shredding Options

| Option | Default | Description |
| --- | --- | --- |
| `variant.shreddingSchema` | unset | Configured shredding schema as a Paimon `ROW` type JSON string. Top-level field names match `VARIANT` column names, and their nested types describe the typed fields to extract. |
| `parquet.variant.shreddingSchema` | unset | Parquet-scoped alias for `variant.shreddingSchema`. |
| `variant.inferShreddingSchema` | `false` | Enables per-writer schema inference for `VARIANT` columns when no configured shredding schema is set. |
| `parquet.variant.inferShreddingSchema` | `false` | Parquet-scoped alias for `variant.inferShreddingSchema`. |
| `variant.shredding.maxInferBufferRow` | `4096` | Number of initial rows buffered per data-file writer before inferring the shredding schema. If fewer rows are written, inference runs when the writer is flushed or closed. |
| `variant.shredding.maxSchemaDepth` | `50` | Maximum nested depth considered by inference. |
| `variant.shredding.maxSchemaWidth` | `300` | Maximum number of inferred typed fields across inferred Variant schemas. |
| `variant.shredding.minFieldCardinalityRatio` | `0.1` | Minimum ratio of sampled non-null Variant values that must contain a field before inference keeps it as a typed field. |

These options apply to `VARIANT` columns written to Parquet. They control the
file layout while preserving the logical `VARIANT` type. Configured shredding
takes precedence over inferred shredding. Tables without `VARIANT` columns, or
without shredding enabled, use the normal file layout.

#### Other Options

| Option | Description |
| --- | --- |
| `'sequence.field' = 'col'` | Sequence field used to determine which record wins during deduplication |
| `'row-tracking.enabled' = 'true'` | Enable stable row ids |
| `'data-evolution.enabled' = 'true'` | Enable data evolution and partial-column writes |
| `'manifest.sidecar.enabled' = 'true'` | Write and read manifest block sidecars for partition, row-id, and bucket pruning; when unset, inherits `manifest-sort.enabled` |
| `'global-index.enabled' = 'true'` | Enable global index metadata and reads |
| `'deletion-vectors.enabled' = 'true'` | Enable deletion vectors |
| `'deletion-vectors.merge-on-read' = 'true'` | Include and key-merge uncompacted level-0 files in DV-enabled deduplicate batch reads |
| `'changelog-producer' = 'input'` | Changelog producer; primary-key tables support reads and writes in this mode |

Cross-partition update mode applies when `'bucket' = '-1'` and the primary key
does not contain every partition column; it has no separate option.
