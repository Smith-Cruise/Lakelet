---
icon: lucide/database
---

# Catalog

Lakelet supports these catalogs:

- [HMS](hms.md)
- [Glue](glue.md)
- [Paimon FileSystem](paimon-filesystem.md)
- [Iceberg REST](iceberg-rest.md)
- [TPC-H](tpch.md)
- [TPC-DS](tpcds.md)

You can attach [storage block](../storage.md) if you want to access authenticated object store.

TPC-H and TPC-DS provide generated, read-only data and require no external storage.

## Multiple catalogs

Catalog blocks are repeatable, you can create multiple catalogs like:

```toml
[[catalog.hms]]
name = "hms_dev"
metastore-uri = "127.0.0.1:9083"

[[catalog.hms]]
name = "hms_prod"
metastore-uri = "hms.example.com:9083"

[[catalog.paimon-fs]]
name = "paimon_lake"
warehouse = "s3://bucket/warehouse"
```
