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

Bring the catalog you already run.

<div class="grid cards" markdown>

-   :lucide-database:{ .lg .middle } __Hive Metastore__

    ---

    The classic metadata service of the big data ecosystem.

    [:lucide-arrow-right: HMS](catalog/hms.md)

-   :lucide-cloud:{ .lg .middle } __AWS Glue__

    ---

    Amazon's fully managed data catalog.

    [:lucide-arrow-right: Glue](catalog/glue.md)

-   :lucide-folder-tree:{ .lg .middle } __Paimon Filesystem__

    ---

    A Paimon warehouse directory, no metastore service needed.

    [:lucide-arrow-right: Paimon FileSystem](catalog/paimon-filesystem.md)

</div>

## Table formats

One engine, four open table formats.

<div class="grid cards" markdown>

-   :lucide-mountain-snow:{ .lg .middle } __Iceberg__

    ---

    An open table format for large-scale analytics.

    [:lucide-arrow-right: Iceberg](table-format/iceberg.md)

-   :lucide-triangle:{ .lg .middle } __Delta Lake__

    ---

    An open table format from the Databricks ecosystem.

    [:lucide-arrow-right: Delta Lake](table-format/delta-lake.md)

-   :lucide-layers:{ .lg .middle } __Paimon__

    ---

    A lake format built for streaming and batch alike.

    [:lucide-arrow-right: Paimon](table-format/paimon.md)

-   :lucide-hexagon:{ .lg .middle } __Hive__

    ---

    The classic table layout of the Hadoop era.

    [:lucide-arrow-right: Hive](table-format/hive.md)

</div>

</div>
