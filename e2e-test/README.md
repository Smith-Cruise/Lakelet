# DobbyDB E2E Tests

This directory contains end-to-end tests for querying lake table formats through
DobbyDB. The shared test framework is prepared for multiple table formats, but
the current implemented case is Paimon.

## Prerequisites

- Python environment: `/Users/smith/Software/py-env`
- Java runtime available on `PATH`
- Rust toolchain available on `PATH`
- Network access for downloading Python packages and test jars

## Run

From the repository root:

```bash
cd /Users/smith/Code/DobbyDB
source /Users/smith/Software/py-env/bin/activate
python e2e-test/run_e2e.py --case paimon
```

The runner will:

- install Python dependencies from `e2e-test/requirements.txt`
- download the Paimon Spark jars into `e2e-test/.jars/`
- build `target/debug/dobbydb` if it does not exist
- start a moto HTTP server endpoint for mocked Glue and S3
- use Spark to write a Paimon table into mocked S3
- run `cases/<format>/*.test` files through DobbyDB and assert the results

## Run After Dependencies Are Installed

Use `--skip-install` to avoid reinstalling Python dependencies:

```bash
source /Users/smith/Software/py-env/bin/activate
python e2e-test/run_e2e.py --case paimon --skip-install
```

## Current Scope

Only `--case paimon` is implemented. The runner already reserves case names for
`hive`, `iceberg`, `delta`, and `all`, but those cases intentionally fail until
their tests are added.

## Test File Format

Each E2E case is a single `.test` file containing metadata plus one or more
`QUERY` / `EXPECTED` blocks.

```sql
-- TEST: paimon_orders
-- DATABASE: paimon_db
-- SETUP: orders

-- QUERY
SELECT count(*) FROM orders;
-- EXPECTED
count(*)
4
```

The runner normalizes DobbyDB table output into comma-separated text before
comparing with `EXPECTED`. Multi-line values, such as `SHOW CREATE TABLE`, are
represented as multiple expected lines.

The current fixed local resources are:

- catalog: `moto_glue`
- bucket: `dobbydb-e2e`
- Paimon database: `paimon_db`
- Hive database: `hive_db`
- Iceberg database: `iceberg_db`
- Delta database: `delta_db`

Setup functions live next to the case files. For example,
`cases/paimon/orders.test` declares `-- SETUP: orders`, so the runner calls
`cases/paimon/setup.py::setup_orders`.

## Glue Registration

The Paimon case currently lets Spark create the Paimon table data and metadata
in mocked S3, then the test framework registers the table in moto Glue for
DobbyDB discovery.

Spark/Paimon can theoretically create the table through a Glue-backed Hive
metastore client, but that path requires the AWS Glue Data Catalog Spark/Hive
client jar. The client is not published as a normal Maven Central artifact, so
this test suite does not build or vendor it locally.
