---
icon: lucide/layers
---

# Paimon

An HMS or Glue table is treated as Paimon when its properties contain
`table_type` with value `PAIMON`, case-insensitively. Tables from a
[Paimon FileSystem catalog](../catalogs/paimon-filesystem.md) are always
Paimon. Lakelet loads the latest Paimon schema from the table location.

## Data Types

Same as paimon-rust.

## Create schema

```sql
CREATE SCHEMA [IF NOT EXISTS] schema_name;
```

## Drop schema

```sql
DROP SCHEMA [IF EXISTS] schema_name [CASCADE | RESTRICT];
```

## Create table

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
    column_definition [, ...]
    [, PRIMARY KEY (column_name [, ...])]
)
[PARTITIONED BY (column_name [, ...])]
[USING PAIMON]
[WITH ('key' = 'value' [, ...])];

column_definition:
    column_name data_type
    [NULL | NOT NULL]
    [COMMENT 'text']
    [PRIMARY KEY]
```

- `IF NOT EXISTS` leaves an existing table unchanged; it does not compare or
  update its definition. Without it, creating an existing table is an error.
- Columns are nullable by default. Primary-key columns become non-nullable and
  cannot be declared explicitly `NULL`. Only one primary key is allowed, either
  inline or as a table constraint.
- `PARTITIONED BY` references existing column names without types.
- `USING PAIMON` is optional. Other table formats are not accepted.
- `WITH` accepts single-quoted string keys and values.
- Table locations are chosen under the configured warehouse.

### Table options

Set Paimon table options with `WITH ('key' = 'value' [, ...])` when creating a
table. Keys and values, including numbers and booleans, must be single-quoted
strings. The options below follow the
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

## Drop table

```sql
DROP TABLE [IF EXISTS] table_name;
```

## Writing tables

Existing Paimon tables support `INSERT INTO` and `INSERT OVERWRITE`. The storage
credentials must allow writes to the table location.

```sql
INSERT INTO table_name [(column_name [, ...])]
{ VALUES (expression [, ...]) [, ...] | query };
```

Braces group alternatives; choose either `VALUES` or a query. Create the
destination table before inserting rows or query results. Creation and insertion
are separate operations; an INSERT failure does not undo table creation.
INSERT results report the number of rows written.

### Overwrite data

```sql
INSERT OVERWRITE table_name [(column_name [, ...])]
{ VALUES (expression [, ...]) [, ...] | query };
```

For an unpartitioned table, `INSERT OVERWRITE` replaces all its data. For a
partitioned table, it replaces only the partitions present in the new data.
Other partitions remain unchanged.

## Metadata Table

Metadata tables expose snapshots, files, options, and other table information.
Append the metadata table name to the table name with a `$` separator:

```sql
SELECT * FROM `table_name$snapshots`;
```

Note that Lakelet names the list of data files `data_files`, consistent with
the other table formats; Paimon's native `files` spelling is not accepted.

**data_files**

Returns one row for each data file in the latest snapshot:

```sql
SELECT * FROM `table_name$data_files`;
```

| Column | Description |
| --- | --- |
| `partition` | Partition of the file, or null for unpartitioned tables |
| `bucket` | Bucket the file belongs to |
| `file_path` | Path of the data file |
| `file_format` | Format of the file, such as `parquet` |
| `schema_id` | ID of the schema the file was written with |
| `level` | LSM tree level of the file |
| `record_count` | Number of records in the file |
| `file_size_in_bytes` | Size of the file in bytes |
| `min_key` | Minimum primary key of the file |
| `max_key` | Maximum primary key of the file |
| `null_value_counts` | Per-column null value counts |
| `min_value_stats` | Per-column minimum values |
| `max_value_stats` | Per-column maximum values |
| `min_sequence_number` | Minimum sequence number in the file |
| `max_sequence_number` | Maximum sequence number in the file |
| `creation_time` | Time the file was created |
| `delete_row_count` | Number of deleted rows tracked for the file |
| `file_source` | How the file was produced, such as `APPEND` or `COMPACT` |
| `first_row_id` | First row ID assigned to the file |
| `write_cols` | Columns the file was written with |

**partitions**

Returns one row for each partition:

```sql
SELECT * FROM `table_name$partitions`;
```

| Column | Description |
| --- | --- |
| `partition` | Partition values |
| `record_count` | Number of records in the partition |
| `file_size_in_bytes` | Total size of the partition's files in bytes |
| `file_count` | Number of files in the partition |
| `last_update_time` | Time the partition was last updated |
| `created_at` | Time the partition was created |
| `created_by` | User that created the partition |
| `updated_by` | User that last updated the partition |
| `options` | Partition-level options |
| `total_buckets` | Number of buckets in the partition |
| `done` | Whether the partition is marked done |

**snapshots**

Returns one row for each snapshot of the table:

```sql
SELECT * FROM `table_name$snapshots`;
```

| Column | Description |
| --- | --- |
| `snapshot_id` | ID of the snapshot |
| `schema_id` | ID of the schema the snapshot was written with |
| `commit_user` | User that committed the snapshot |
| `commit_identifier` | Commit identifier of the snapshot |
| `commit_kind` | Kind of the commit, such as `APPEND` or `COMPACT` |
| `commit_time` | Time the snapshot was committed |
| `base_manifest_list` | Path of the base manifest list |
| `delta_manifest_list` | Path of the delta manifest list |
| `changelog_manifest_list` | Path of the changelog manifest list, if any |
| `total_record_count` | Total number of records in the snapshot |
| `delta_record_count` | Number of records added by the snapshot |
| `changelog_record_count` | Number of changelog records in the snapshot |
| `watermark` | Watermark of the snapshot |
| `next_row_id` | Next row ID after the snapshot |

**manifests**

Returns one row for each manifest file of the latest snapshot:

```sql
SELECT * FROM `table_name$manifests`;
```

| Column | Description |
| --- | --- |
| `file_name` | Name of the manifest file |
| `file_size` | Size of the manifest file in bytes |
| `num_added_files` | Number of files added by the manifest |
| `num_deleted_files` | Number of files deleted by the manifest |
| `schema_id` | ID of the schema the manifest was written with |
| `min_partition_stats` | Minimum partition statistics |
| `max_partition_stats` | Maximum partition statistics |
| `min_row_id` | Minimum row ID covered by the manifest |
| `max_row_id` | Maximum row ID covered by the manifest |

**options**

Returns the table options that are set explicitly, one row per option:

```sql
SELECT * FROM `table_name$options`;
```

| Column | Description |
| --- | --- |
| `key` | Option key |
| `value` | Option value |

**schemas**

Returns one row for each schema version of the table:

```sql
SELECT * FROM `table_name$schemas`;
```

| Column | Description |
| --- | --- |
| `schema_id` | ID of the schema |
| `fields` | Fields of the schema, as a JSON string |
| `partition_keys` | Partition keys, as a JSON string |
| `primary_keys` | Primary keys, as a JSON string |
| `options` | Options recorded in the schema, as a JSON string |
| `comment` | Table comment |
| `update_time` | Time the schema was updated |

**tags**

Returns one row for each tag of the table:

```sql
SELECT * FROM `table_name$tags`;
```

| Column | Description |
| --- | --- |
| `tag_name` | Name of the tag |
| `snapshot_id` | ID of the tagged snapshot |
| `schema_id` | ID of the schema of the tagged snapshot |
| `commit_time` | Commit time of the tagged snapshot |
| `record_count` | Number of records in the tagged snapshot |
| `create_time` | Time the tag was created |
| `time_retained` | How long the tag is retained |

**branches**

Returns one row for each branch of the table:

```sql
SELECT * FROM `table_name$branches`;
```

| Column | Description |
| --- | --- |
| `branch_name` | Name of the branch |
| `create_time` | Time the branch was created |

**table_indexes**

Returns one row for each committed global index file, including BTree
indexes, vector indexes, and deletion-vector metadata:

```sql
SELECT * FROM `table_name$table_indexes`;
```

| Column | Description |
| --- | --- |
| `partition` | Partition of the index file, or null |
| `bucket` | Bucket of the index file |
| `index_type` | Type of the index |
| `file_name` | Name of the index file |
| `file_size` | Size of the index file in bytes |
| `row_count` | Number of rows covered by the index |
| `dv_ranges` | Deletion-vector ranges, as a list of structs |
| `row_range_start` | Start of the row range covered by the index |
| `row_range_end` | End of the row range covered by the index |
| `index_field_id` | ID of the indexed field |
| `index_field_name` | Name of the indexed field |

**physical_files_size**

Returns a single row with the physical disk usage of the table, categorized
by manifest, data, and index files:

```sql
SELECT * FROM `table_name$physical_files_size`;
```

| Column | Description |
| --- | --- |
| `manifest_file_count` | Number of manifest files |
| `manifest_file_size` | Total size of manifest files in bytes |
| `data_file_count` | Number of data files |
| `data_file_size` | Total size of data files in bytes |
| `index_file_count` | Number of index files |
| `index_file_size` | Total size of index files in bytes |

**aggregation_fields**

Returns one row per table field, including its field-specific options and comment:

```sql
SELECT * FROM `table_name$aggregation_fields`;
```

| Column | Description |
| --- | --- |
| `field_name` | Field name |
| `field_type` | Paimon field type |
| `function` | Values of the field-specific options, rendered as a list |
| `function_options` | Corresponding `fields.<field_name>.*` option keys, rendered as a list |
| `comment` | Field comment, or null |

Fields without these options have `[]` in the two option columns. For example,
an option `'fields.total.aggregate-function' = 'sum'` appears as `[sum]` in
`function` and `[fields.total.aggregate-function]` in `function_options`.

**audit_log**

Exposes data rows with a leading `rowkind` column. This describes row kinds in
the data, not a history of SQL statements, users, or access events. A normal
query reads the current table state; it is not a complete history of changes.

```sql
SELECT * FROM `table_name$audit_log` LIMIT 10;
```

| Column | Description |
| --- | --- |
| `rowkind` | Row kind: `+I` (insert), `-U` (update before), `+U` (update after), or `-D` (delete) |
| Table columns | Columns from the underlying table |

The row kinds returned depend on the table's data and read behavior. If the
table enables `table-read.sequence-number.enabled`, an additional
`_SEQUENCE_NUMBER` column follows `rowkind`.

**buckets**

Returns file statistics grouped by partition and bucket in the current snapshot:

```sql
SELECT * FROM `table_name$buckets`;
```

| Column | Description |
| --- | --- |
| `partition` | Partition values, or null for an unpartitioned table |
| `bucket` | Bucket ID |
| `record_count` | Sum of record counts in the bucket's data files |
| `file_size_in_bytes` | Total size of those files in bytes |
| `file_count` | Number of data files |
| `last_update_time` | Latest file creation time in the bucket, or null |

**consumers**

Returns the consumer progress recorded for the table. Querying this metadata
table does not create or advance a consumer.

```sql
SELECT * FROM `table_name$consumers`;
```

| Column | Description |
| --- | --- |
| `consumer_id` | Consumer identifier |
| `next_snapshot_id` | Next snapshot the consumer should read |

**referenced_files_size**

Returns file counts and sizes aggregated across all referenced snapshots,
including branches and tags, one row per source:

```sql
SELECT * FROM `table_name$referenced_files_size`;
```

| Column | Description |
| --- | --- |
| `source` | Where the files are referenced from |
| `manifest_file_count` | Number of manifest files |
| `manifest_file_size` | Total size of manifest files in bytes |
| `data_file_count` | Number of data files |
| `data_file_size` | Total size of data files in bytes |
| `index_file_count` | Number of index files |
| `index_file_size` | Total size of index files in bytes |
