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
  `lakelet --flight-sql-server` process for the whole session and opens one
  connection per test file, passing the file's `-- DATABASE` via the
  `default-catalog`/`default-schema` gRPC headers.
- Docker must be running.
- Ports `5050` (moto) and `32010` (Flight SQL server) on `127.0.0.1` must be
  free.
- Test jars are downloaded into `integration-tests/.jars/`.
- Default `cargo test` does not run these integration tests.
- Use `--keep-compose` to keep docker-compose containers after the script exits:

```bash
./integration-tests/integration-test.sh --keep-compose
```
