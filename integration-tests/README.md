# DobbyDB Integration Tests

## Run

Run from the repository root:

```bash
./integration-tests/integration-test.sh
```

Use your own Python environment before running the script. The script calls
`python3` directly and installs `integration-tests/requirements.txt` into that
environment.

## Notes

- Docker must be running.
- Port `5050` on `127.0.0.1` must be free.
- Test jars are downloaded into `integration-tests/.jars/`.
- Default `cargo test` does not run these integration tests.
- Use `--keep-compose` to keep docker-compose containers after the script exits:

```bash
./integration-tests/integration-test.sh --keep-compose
```
