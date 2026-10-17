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
