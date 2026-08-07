---
icon: lucide/mountain-snow
---

# Iceberg

An HMS or Glue table is treated as Iceberg when its properties contain
`metadata_location`. Lakelet loads the table directly from that metadata file.

Only Parquet data files are currently supported.

## Data Types

The same as `iceberg-rust`.

## Metadata Table

Iceberg metadata tables are served by `iceberg-rust`, so their columns follow
that crate's schema rather than one defined by Lakelet.

**snapshots**

Returns one row for each snapshot in the table metadata:

```sql
SELECT * FROM `table_name$snapshots`;
```

| Column | Description |
| --- | --- |
| `committed_at` | Time the snapshot was committed, as a UTC timestamp |
| `snapshot_id` | ID of the snapshot |
| `parent_id` | ID of the parent snapshot, or null for the first snapshot |
| `operation` | Operation that produced the snapshot, such as `append` or `overwrite` |
| `manifest_list` | Path of the snapshot's manifest list file |
| `summary` | Snapshot summary properties, as a string-to-string map |

**manifests**

Returns one row for each manifest file in the **current** snapshot's manifest
list. A table with no current snapshot returns no rows:

```sql
SELECT * FROM `table_name$manifests`;
```

| Column | Description |
| --- | --- |
| `content` | Type of content the manifest tracks: `0` for data, `1` for deletes |
| `path` | Path of the manifest file |
| `length` | Size of the manifest file in bytes |
| `partition_spec_id` | ID of the partition spec used to write the manifest |
| `added_snapshot_id` | ID of the snapshot that added the manifest |
| `added_data_files_count` | Number of data files added in the manifest |
| `existing_data_files_count` | Number of existing data files in the manifest |
| `deleted_data_files_count` | Number of data files deleted in the manifest |
| `added_delete_files_count` | Number of delete files added in the manifest |
| `existing_delete_files_count` | Number of existing delete files in the manifest |
| `deleted_delete_files_count` | Number of delete files deleted in the manifest |
| `partition_summaries` | Per-partition-field summaries, as a list of structs with `contains_null`, `contains_nan`, `lower_bound`, and `upper_bound` |

**history**

Returns one row for each entry in the table's snapshot log, including entries
that are no longer part of the current lineage after a rollback:

```sql
SELECT * FROM `table_name$history`;
```

| Column | Description |
| --- | --- |
| `made_current_at` | Time the snapshot became the current snapshot, as a UTC timestamp |
| `snapshot_id` | ID of the snapshot |
| `parent_id` | ID of the parent snapshot, or null for the first snapshot |
| `is_current_ancestor` | Whether the snapshot is an ancestor of the current snapshot; `false` marks rolled-back entries |