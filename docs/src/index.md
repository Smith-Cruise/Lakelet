---
hide:
  - navigation
  - toc
---

# Focused on Data Lake Queries

One binary that speaks Iceberg, Delta Lake, Paimon, and Hive.
Point it at your catalog and start writing SQL — no Spark, no JVM, no cluster.

```sh
curl -fsSL https://lakelet.dev/install.sh | sh
```

Linux and macOS. For Windows, grab a build from
[releases/nightly](https://github.com/Smith-Cruise/Lakelet/releases/tag/nightly).

[Get started](getting-started.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/Smith-Cruise/Lakelet){ .md-button }

## Table formats

Lakelet determines a table format from metastore table properties, discovered
through HMS or Glue.

<div class="grid cards" markdown>

-   :material-table:{ .lg .middle } __Iceberg__

    ---

    Loaded directly from the `metadata_location` property. Metadata tables are
    served by `iceberg-rust`.

    [:octicons-arrow-right-24: Iceberg](table-format/iceberg.md)

-   :material-delta:{ .lg .middle } __Delta Lake__

    ---

    Read from the table location through `delta-rs`. Experimental — support is
    not yet complete.

    [:octicons-arrow-right-24: Delta Lake](table-format/delta-lake.md)

-   :material-layers-triple:{ .lg .middle } __Paimon__

    ---

    The latest Paimon schema is loaded from the table location and read through
    `paimon-datafusion`.

    [:octicons-arrow-right-24: Paimon](table-format/paimon.md)

-   :material-elephant:{ .lg .middle } __Hive__

    ---

    TextFile and Parquet data files are supported. ORC is not supported yet.

    [:octicons-arrow-right-24: Hive](table-format/hive.md)

</div>
