---
icon: lucide/layers
---

# Paimon

An HMS or Glue table is treated as Paimon when its properties contain
`table_type` with value `PAIMON`, case-insensitively.

## Data Types

Same as `paimon-rust`.

## Supported Operations

| Operation | Notes |
| --- | --- |
| `SELECT` | |
| [`DESCRIBE`](../sql/describe.md) | |
| [`SHOW CREATE TABLE`](../sql/show-create-table.md) | |
| [`CREATE SCHEMA`](../sql/create-schema.md) | Paimon FileSystem catalog only. |
| [`DROP SCHEMA`](../sql/drop-schema.md) | Paimon FileSystem catalog only. |
| [`CREATE TABLE`](../sql/create-table.md) | Paimon FileSystem catalog only. Supports `AS SELECT` and `LIKE`. |
| [`DROP TABLE`](../sql/drop-table.md) | Paimon FileSystem catalog only. |
| [`INSERT INTO`](../sql/insert.md) | |
| [`INSERT OVERWRITE`](../sql/insert.md) | Replaces only the partitions present in the input. For an unpartitioned table, it replaces all rows. |
| [`CALL PROCEDURE`](../sql/call-procedure.md) | Paimon FileSystem catalog only. |

## Supported Call Procedures

Use `CALL paimon.<procedure>` on a table in a [Paimon FileSystem catalog](../catalogs/paimon-filesystem.md).
Every procedure takes a named `table` argument. For example, to tag the latest snapshot:

```sql
CALL paimon.create_tag(table => 'paimon_local.sales.orders', tag => 'daily_close');
```

The table lists the remaining named arguments. Brackets mark optional arguments.

| Procedure | Other named arguments | What it does |
| --- | --- | --- |
| `create_tag` | `tag`, `[snapshot_id]`, `[time_retained]` | Tags a snapshot. Omitting `snapshot_id` selects the latest snapshot. |
| `create_tag_from_timestamp` | `tag`, `timestamp` | Tags the earliest retained snapshot committed at or after the given Unix timestamp in milliseconds. |
| `create_tag_from_watermark` | `tag`, `watermark`, `[time_retained]` | Tags the snapshot with the lowest watermark at or above the given value in milliseconds. |
| `replace_tag` | `tag`, `[snapshot_id]`, `[time_retained]` | Moves an existing tag to the specified snapshot, or to the latest snapshot if omitted. |
| `rename_tag` | `tag`, `target_tag` | Renames an existing tag. |
| `delete_tag` | `tag` | Deletes one tag or a comma-separated list of tags. |
| `create_branch` | `branch`, `[tag]`, `[ignore_if_exists]` | Creates a branch from the main table's schema. Supply a tag to start from its snapshot. |
| `rename_branch` | `from_branch`, `to_branch` | Renames an existing branch. |
| `delete_branch` | `branch` | Deletes one branch or a comma-separated list of branches. |
| `rollback_to` | `snapshot_id` or `tag` | Restores the table to the specified snapshot or the snapshot referenced by the tag. |
| `rollback_to_timestamp` | `timestamp` | Restores the latest retained snapshot committed at or before the given Unix timestamp in milliseconds. |
| `reset_consumer` | `consumer_id`, `[next_snapshot_id]` | Sets the next snapshot a consumer will read. Omitting `next_snapshot_id` deletes its saved position. |
| `clear_consumers` | `[including_consumers]`, `[excluding_consumers]` | Clears saved positions for matching consumer IDs and skips excluded IDs. Omitting both patterns clears all positions for the table. |
| `create_global_index` | `index_column`, `[index_type]`, `[options]` | Builds a global index on a column. The default index type is `btree`. |
| `drop_global_index` | `index_column`, `[index_type]`, `[partitions]`, `[dry_run]` | Drops the column's index for the selected partitions or the whole table. Use `dry_run => true` to preview the change. |
| `compact` | `[partitions]` | Compacts eligible data files across the table or in the selected partitions. |

See [CALL PROCEDURE](../sql/call-procedure.md) for Lakelet's procedure syntax and the
[Paimon Rust procedure reference](https://paimon.apache.org/docs/rust/sql/#procedures) for more examples.

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
