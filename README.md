# DobbyDB

**Wake up, query the data lake.**

DobbyDB is a single-binary SQL engine for lakehouse tables. No Spark cluster is
needed: point it at Hive Metastore or AWS Glue, then query Iceberg, Hive,
Paimon, and Delta Lake tables directly with SQL.

```sql
show catalogs;
use catalog hms;
select * from default.orders limit 10;
```

## Features

| Area | Support |
| --- | --- |
| Catalog | Hive Metastore, AWS Glue |
| Table format | Iceberg, Hive, Paimon, Delta Lake |
| File format | Parquet, Hive TextFile |
| Storage | S3 / S3-compatible storage, Aliyun OSS, HDFS |

Delta Lake support is still incomplete. See the docs for detailed limitations.

## Quick Start

```bash
cargo build --release
cp config_demo.toml config.toml
```

Configure at least one HMS or Glue catalog in `config.toml`:

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

Start the SQL shell:

```bash
target/release/dobbydb --config config.toml
```

Run one SQL command and exit:

```bash
target/release/dobbydb --config config.toml --command "show catalogs;"
```

Run SQL from a file:

```bash
target/release/dobbydb --config config.toml --file query.sql
```

## Basic SQL

```sql
show catalogs;
use catalog hms;
show schemas;
use default;
show tables;
select * from my_table limit 10;
```

You can also use fully qualified table names:

```sql
select * from hms.default.my_table limit 10;
```

## Documentation

https://smith-cruise.github.io/DobbyDB/
