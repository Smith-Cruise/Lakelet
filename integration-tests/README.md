# Lakelet Integration Tests

## Run

Run from the repository root:

```bash
./integration-tests/integration-test.sh
```

Use your own Python environment before running the script. The script calls
`python3` directly and installs `integration-tests/requirements.txt` into that
environment.

## Notes

- Queries run over ADBC (`adbc_driver_flightsql`): pytest starts one
  `lakelet --server` process for the whole session and opens one
  connection per test file, passing `-- CATALOG` (default: `moto_glue`) and
  `-- DATABASE` via the `default-catalog`/`default-schema` gRPC headers.
  Connections explicitly use autocommit, and queries run through the DB-API `cursor.execute()` interface;
  prepared statements and parameter binding are not supported.
- Docker must be running.
- Ports `5050` (moto), `8181` (Iceberg REST catalog) and `32010` (Flight SQL
  server) on `127.0.0.1` must be free.
- Test jars are downloaded into `integration-tests/.jars/`.
- Default `cargo test` does not run these integration tests.
- Each `-- QUERY` must have an explicit `-- EXPECTED` section. Leave it empty
  for DDL with no result columns; a query returning zero rows still includes
  its column header. INSERT results include the affected-row count.
- `paimon-write.test` creates and drops its own `lakelet_write_db` schema. It
  reads the Spark-generated fixture without modifying it, and tests Lakelet
  append, partitioned writes, overwrite, metadata, and cleanup.
- Use `--keep-compose` to keep docker-compose containers after the script exits:

```bash
./integration-tests/integration-test.sh --keep-compose
```
