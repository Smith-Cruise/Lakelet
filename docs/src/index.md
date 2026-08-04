---
icon: lucide/house
hide:
  - navigation
  - toc
---

<div class="ll-home" markdown>

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

## Catalogs

Connect Lakelet to the metastore you already run — tables are discovered
through Hive Metastore or AWS Glue.

<div class="grid cards" markdown>

-   :lucide-database:{ .lg .middle } __Hive Metastore__

    ---

    Connect to any Hive Metastore over Thrift with a single `metastore-uri`.
    Repeat the block to add as many catalogs as you need.

    [:lucide-arrow-right: HMS](catalog.md#hms)

-   :lucide-cloud:{ .lg .middle } __AWS Glue__

    ---

    Point Lakelet at the AWS Glue Data Catalog. Static keys or the default
    AWS credential chain — both just work.

    [:lucide-arrow-right: Glue](catalog.md#glue)

</div>

## Table formats

Lakelet determines a table format from metastore table properties, discovered
through HMS or Glue.

<div class="grid cards" markdown>

-   :lucide-mountain-snow:{ .lg .middle } __Iceberg__

    ---

    Loaded directly from the `metadata_location` property. Metadata tables are
    served by `iceberg-rust`.

    [:lucide-arrow-right: Iceberg](table-format/iceberg.md)

-   :lucide-triangle:{ .lg .middle } __Delta Lake__

    ---

    Read from the table location through `delta-rs`. Experimental — support is
    not yet complete.

    [:lucide-arrow-right: Delta Lake](table-format/delta-lake.md)

-   :lucide-layers:{ .lg .middle } __Paimon__

    ---

    The latest Paimon schema is loaded from the table location and read through
    `paimon-datafusion`.

    [:lucide-arrow-right: Paimon](table-format/paimon.md)

-   :lucide-hexagon:{ .lg .middle } __Hive__

    ---

    TextFile and Parquet data files are supported. ORC is not supported yet.

    [:lucide-arrow-right: Hive](table-format/hive.md)

</div>

</div>
