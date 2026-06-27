# DobbyDB Integration Tests

This directory contains docker-compose based integration tests for querying lake
table formats through DobbyDB. The current implemented case is Paimon.

## Run

From the repository root:

```bash
./integration-tests/integration-test.sh
```

The script will:

- install Python dependencies into the current `python3` environment
- download Paimon jars into `integration-tests/.jars`
- start moto and Spark with docker-compose
- build `target/debug/dobbydb`
- run `provision.py` to create Paimon data through Spark
- register the Paimon table in moto Glue without Glue columns or partition keys
- run `cases/*.test` files through pytest and assert the results

Default `cargo test` does not run these tests. Integration tests are only run
through `./integration-tests/integration-test.sh`.

## Current Scope

Only Paimon is implemented. Hive, Iceberg, and Delta can reuse the same layout:
format-specific setup in `provision.py` and assertions under `cases`.

## Test File Format

Each case is a single `.test` file containing metadata plus one or more
`QUERY` / `EXPECTED` blocks.

```sql
-- TEST: paimon_orders
-- DATABASE: paimon_db

-- QUERY
SELECT count(*) FROM orders;
-- EXPECTED
count(*)
4
```

The pytest runner normalizes DobbyDB table output into comma-separated text before
comparing with `EXPECTED`. Multi-line values, such as `SHOW CREATE TABLE`, are
represented as multiple expected lines.

The current fixed local resources are:

- catalog: `moto_glue`
- bucket: `dobbydb-e2e`
- Paimon database: `paimon_db`
- host moto endpoint: `http://127.0.0.1:5050`
- Spark container moto endpoint: `http://moto:5000`

Set `KEEP_COMPOSE=1` to keep containers running after the script exits for
debugging.
