# Benchmark

This directory contains benchmark SQL files and the Python benchmark runner.

Currently supported benchmark suite:

- `tpch`

Currently supported engines:

- `lakelet`
- `starrocks`

## Setup

Install the benchmark dependencies:

```bash
python3 -m pip install -r benchmark/requirements.txt
```

Lakelet benchmarks use the ADBC Flight SQL driver and PyArrow. StarRocks
benchmarks use PyMySQL.

## Common Options

The runner requires these options for all engines:

- `--engine`: benchmark engine, such as `lakelet` or `starrocks`.
- `--benchmark-type`: benchmark suite. Currently only `tpch` is supported.
- `--default-catalog`: default catalog for the benchmark session.
- `--default-schema`: default schema or database for the benchmark session.

Optional common options:

- `--runs`: runs per query. Defaults to `1`.
- `--query`: run only one query, such as `q01`. This option can be specified
  multiple times.
- `--output`: parent directory for benchmark output artifacts. Defaults to
  `/tmp`.

## Lakelet

Run one query:

```bash
python3 benchmark/run_benchmark.py \
  --engine lakelet \
  --benchmark-type tpch \
  --default-catalog hms_catalog \
  --default-schema tpch_hive_sf1 \
  --runs 1 \
  --query q01 \
  --bin target/debug/lakelet \
  --config config.toml
```

Run the full TPC-H suite:

```bash
python3 benchmark/run_benchmark.py \
  --engine lakelet \
  --benchmark-type tpch \
  --default-catalog hms_catalog \
  --default-schema tpch_hive_sf1 \
  --runs 1 \
  --bin target/debug/lakelet \
  --config config.toml
```

The Lakelet runner starts one `lakelet --flight-sql-server` process for the
whole benchmark and reuses one ADBC Flight SQL connection for every query run.
The connection sends the requested default catalog and schema as Flight SQL
request headers.

The runner reads the Flight SQL port from `flight-sql-server-port` under the
config file's `[server]` table. It uses port `32010` when the option is omitted.
The configured port must be free before the benchmark starts.

Elapsed time starts immediately before ADBC executes the SQL and ends after the
complete Arrow result has been fetched. Server startup and result formatting
are not included.

## StarRocks

Run one query:

```bash
python3 benchmark/run_benchmark.py \
  --engine starrocks \
  --benchmark-type tpch \
  --default-catalog default_catalog \
  --default-schema tpch_hive_sf1 \
  --runs 1 \
  --query q01 \
  --host 127.0.0.1 \
  --port 9030 \
  --user root
```

Use an environment variable when a password is required:

```bash
STARROCKS_PASSWORD='your-password' \
python3 benchmark/run_benchmark.py \
  --engine starrocks \
  --benchmark-type tpch \
  --default-catalog default_catalog \
  --default-schema tpch_hive_sf1 \
  --host 127.0.0.1 \
  --user root \
  --password "$STARROCKS_PASSWORD"
```

The StarRocks runner connects through the MySQL-compatible protocol and runs:

```sql
USE `<default-catalog>`.`<default-schema>`;
```

before executing benchmark SQL files.

## Output

The runner prints one row per query and a summary:

```text
query    |    run1(s) |     avg(s)
---------+------------+-----------
Running q01...
q01      |      2.862 |      2.862

Total queries: 1
Successful runs: 1
Failed runs: 0
Total elapsed(s): 2.862
Average per run(s): 2.862
Benchmark output: /tmp/lakelet-benchmark-20260425-153000-12345
```

Each benchmark run creates a timestamped directory under `--output` with:

- `console.txt`: benchmark console output.
- `results.csv`: query, run index, status, elapsed seconds, and error message.
- `raw/<query>/run<N>.txt`: SQL text and SQL output result for each run.
- `lakelet-server.log`: Lakelet Flight SQL server output. This file is created
  only for Lakelet benchmarks.

If one SQL run fails, the runner records it as `failed`, prints a `Skip ...`
message, and continues with the remaining runs.
