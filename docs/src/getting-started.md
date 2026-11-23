---
icon: lucide/rocket
---

# Getting Started

Install Lakelet, configure a catalog, and run your first SQL query.

If you use an AI agent, give it [llms.txt](https://lakelet.dev/llms.txt).
It lists the Markdown version of each guide so your agent can read the pages it needs.

## Install the Prebuilt Binary { #install-prebuilt-binary }

Run the installation script to download the latest nightly build.

```bash
curl -fsSL https://lakelet.dev/install.sh | sh
```

The script supports Linux and macOS (x86_64 and aarch64).

Alternatively, download an archive directly from
[GitHub Releases](https://github.com/Smith-Cruise/Lakelet/releases) — this is
also the way to get the Windows build
(`lakelet-nightly-x86_64-pc-windows-msvc.zip`).

## Build from Source

Lakelet requires a Rust toolchain. Build the release binary from the git repository:

```bash
git clone https://github.com/Smith-Cruise/Lakelet.git
cd Lakelet
cargo build --release
cp target/release/lakelet .
```

To include the web UI, build `/web` first (needs Node.js and
[pnpm](https://pnpm.io/)):

```bash
pnpm -C web install
pnpm -C web build
cargo build --release
```

## Create a Configuration File

Copy the example configuration and edit it for your environment:

```bash
cp config_demo.toml config.toml
```

A configuration file can contain the following top-level sections:

| Section | Required | Description |
| --- | --- | --- |
| `[server]` | No | Server-wide settings. |
| `[[catalog.hms]]` | No | A Hive Metastore catalog. |
| `[[catalog.glue]]` | No | An AWS Glue catalog. |
| `[[catalog.paimon-fs]]` | No | A Paimon filesystem catalog. |
| `[[catalog.iceberg-rest]]` | No | An Iceberg REST catalog. |
| `[[catalog.tpch]]` | No | Generated TPC-H data. |
| `[[catalog.tpcds]]` | No | Generated TPC-DS data. |

The built-in `internal` catalog is always available, 
even when no external catalog is configured.

To try Lakelet without an external data source, add the TPC-H catalog to your
configuration.

```toml
[[catalog.tpch]]
name = "tpch"
```

Start the CLI and query the TPC-H data:

```bash
./lakelet --config config.toml --command "SELECT * FROM tpch.sf1.nation LIMIT 10;"
```

See [TPC-H](catalogs/tpch.md) and [TPC-DS](catalogs/tpcds.md) for available tables
and scales.

The following is a minimal HMS configuration:

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
```

Add an `s3-storage` or `oss-storage` entry to the catalog when its tables use object storage:

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

See [Server](server.md), [Catalog](catalogs/index.md), and [Storage](storage.md) for
the complete configuration reference.

## Lakelet CLI

Pass the configuration file with `--config`:

```bash
./lakelet --config config.toml
```

The configuration file is required for normal execution.

Run `./lakelet --help` to list the available options.

Enter statements ending with `;`, and use `quit;` to exit.

The interactive CLI displays at most the first **1000 rows**, with a notice
inside the table when results are truncated. Use SQL `LIMIT` to request
a smaller result.

To run one statement or a SQL file and exit:

```bash
./lakelet --config config.toml --command "SHOW CATALOGS;"
./lakelet --config config.toml --file query.sql
```

`--command` and `--file` do not apply the interactive 1000-row preview limit.
A file can contain multiple statements separated by semicolons; they run in order
in the same session.

## Start the Server

Start Lakelet with `--server` to use the web UI or connect through
[Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html),
including from an ADBC client. Both use the same port.

```bash
lakelet --config config.toml --server
```

```text
Lakelet server is running
  Flight SQL  grpc://localhost:32010
  Web UI      http://localhost:32010
```

The port is `server-port` under `[server]` (default 32010).

### Web UI

Open `http://localhost:32010/` in your browser.

### Flight SQL

Each Flight SQL query uses a fresh session. `USE` and `SET` changes do not carry
over to the next query. 

Use fully qualified table names, or set `default-catalog` and `default-schema`
as needed in the request headers.

### Connect with ADBC (Python)

Lakelet works with the [ADBC](https://arrow.apache.org/adbc/) Flight SQL
driver:

```bash
pip install adbc_driver_flightsql pyarrow
```

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://127.0.0.1:32010", autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute("select 1 as a")
        print(cur.fetch_arrow_table())
```

Parameter binding (`cur.execute(sql, params)`) is not supported.

Every request may carry `default-catalog` and/or `default-schema` in gRPC
metadata headers.

```python
import adbc_driver_flightsql.dbapi as flight_sql
from adbc_driver_flightsql import DatabaseOptions

HEADER = DatabaseOptions.RPC_CALL_HEADER_PREFIX.value

with flight_sql.connect(
    "grpc://127.0.0.1:32010",
    db_kwargs={
        HEADER + "default-catalog": "hive",
        HEADER + "default-schema": "sales",
    },
    autocommit=True,
) as conn:
    with conn.cursor() as cur:
        cur.execute("select * from orders limit 10")
        print(cur.fetch_arrow_table())
```

### Connect with dft

Example using the [datafusion-dft](https://github.com/datafusion-contrib/datafusion-dft)
CLI/TUI (installed with the `flightsql` feature):

```toml
# ~/.config/dft/config.toml
[flightsql_client]
connection_url = "http://127.0.0.1:32010"
```

```bash
dft -c "select 1" --flightsql            # CLI
dft                                      # TUI: switch to the FlightSQL tab
```
