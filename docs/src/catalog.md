---
icon: lucide/database
---

# Catalog

Lakelet support to connect HMS, Glue and Paimon filesystem catalog.

Storage settings are embedded directly in each catalog entry. See
[Storage](storage.md) for their complete field reference.

## Catalog SQL

Lakelet supports these catalog-related SQL statements:

```sql
-- List all catalogs.
show catalogs;

-- Change the current catalog.
use catalog hms_prod;

-- Change both the current catalog and current database.
use hms_prod.default;
```

## HMS

Use `[[catalog.hms]]` to connect to a Hive Metastore through Thrift.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used by Lakelet. |
| `metastore-uri` | String | Yes | None | Hive Metastore address in `host:port` form. |
| `s3-storage` | Inline table | No | Not configured | Credentials and endpoint for tables stored at `s3://` or `s3a://` locations. |
| `oss-storage` | Inline table | No | Not configured | Credentials and endpoint for tables stored at `oss://` locations. |

```toml
[[catalog.hms]]
name = "hms_prod"
metastore-uri = "hms.example.com:9083"
s3-storage = { region = "us-east-1", access-key = "access-key", secret-key = "secret-key" }
```

The metastore URI is resolved as a socket address when the catalog is accessed.
It must not include a URI scheme such as `thrift://`.

Hive tables stored at `hdfs://` locations do not need a storage configuration
block. Lakelet reads the NameNode host and port from the table location. Kerberos
is not support yet.

## Glue

Use `[[catalog.glue]]` to connect to the AWS Glue Data Catalog.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used by Lakelet. |
| `aws-glue-region` | String | No | AWS SDK default | AWS region containing the Glue catalog. |
| `aws-glue-access-key` | String | No | AWS SDK default credential chain | Static access key used for Glue requests. |
| `aws-glue-secret-key` | String | No | AWS SDK default credential chain | Static secret key used for Glue requests. |
| `aws-glue-endpoint` | String | No | AWS SDK default | Custom endpoint URL used for Glue API requests. |
| `s3-storage` | Inline table | No | Not configured | Credentials and endpoint used to read table data at `s3://` or `s3a://` locations. |
| `oss-storage` | Inline table | No | Not configured | Credentials and endpoint used to read table data at `oss://` locations. |

Static Glue credentials are used only when both `aws-glue-access-key` and
`aws-glue-secret-key` are present. If either value is missing, the AWS SDK
default credential chain and region resolution are used.

```toml
[[catalog.glue]]
name = "glue_prod"
aws-glue-region = "us-west-2"
aws-glue-access-key = "access-key"
aws-glue-secret-key = "secret-key"
aws-glue-endpoint = "http://127.0.0.1:4566"
s3-storage = { region = "us-west-2", access-key = "access-key", secret-key = "secret-key" }
```

## Paimon Filesystem

Use `[[catalog.paimon-fs]]` to connect to a
[Paimon filesystem catalog](https://paimon.apache.org/docs/master/concepts/catalog/):
a warehouse directory that stores all metadata directly on the filesystem,
without a metastore service. This catalog serves Paimon tables only.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used by Lakelet. |
| `warehouse` | String | Yes | None | Warehouse root path, e.g. `s3://bucket/warehouse` or a local absolute path. |
| `s3-storage` | Inline table | No | Not configured | Credentials and endpoint for a warehouse at an `s3://` or `s3a://` path. |
| `oss-storage` | Inline table | No | Not configured | Credentials and endpoint for a warehouse at an `oss://` path. |

```toml
[[catalog.paimon-fs]]
name = "paimon_prod"
warehouse = "s3://bucket/warehouse"
s3-storage = { region = "us-east-1", access-key = "access-key", secret-key = "secret-key" }
```

A local warehouse needs no storage block at all:

```toml
[[catalog.paimon-fs]]
name = "paimon_local"
warehouse = "/data/paimon/warehouse"
```

The warehouse follows the Paimon filesystem catalog layout: each database is a
`<database>.db` directory under the warehouse, and each table a directory
beneath it (`warehouse/<database>.db/<table>`). Databases and tables are
discovered by listing those directories, so `show schemas` reports every
`*.db` directory — including non-Paimon ones if the warehouse path is shared
with other data.

## Multiple catalogs

Catalog blocks are repeatable, you can create multiple catalogs like:

```toml
[[catalog.hms]]
name = "hms_dev"
metastore-uri = "127.0.0.1:9083"

[[catalog.hms]]
name = "hms_prod"
metastore-uri = "hms.example.com:9083"

[[catalog.glue]]
name = "glue_analytics"
aws-glue-region = "us-east-1"

[[catalog.paimon-fs]]
name = "paimon_lake"
warehouse = "s3://bucket/warehouse"
```
