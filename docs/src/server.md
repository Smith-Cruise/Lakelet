---
icon: lucide/server
---

# Server

The optional `[server]` table configures the DataFusion query engine runtime.

```toml
[server]
memory-limit = "4GB"
flight-sql-server-port = 32010
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `memory-limit` | String | No | 80% of effective total memory | Caps the memory available to the query engine. |
| `flight-sql-server-port` | Integer | No | 32010 | Port the Arrow Flight SQL server started by `--flight-sql-server` listens on. |

The `memory-limit` value is an integer with an optional, case-insensitive unit. With no unit, the value is treated as bytes.

| Unit | Meaning |
| --- | --- |
| `b` | Bytes |
| `k`, `kb` | Kibibytes |
| `m`, `mb` | Mebibytes |
| `g`, `gb` | Gibibytes |
| `t`, `tb` | Tebibytes |

When `memory-limit` is omitted, Lakelet calculates it once at startup as 80%
of the effective total memory. If Lakelet cannot determine a
valid capacity, startup fails and `memory-limit` must be configured explicitly.

## Arrow Flight SQL server

Lakelet can run as an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
server, including ADBC instead of the interactive REPL:

```bash
lakelet --config config.toml --flight-sql-server
```

The server listens on `flight-sql-server-port` under `[server]` (default
32010).

Note: Each flight SQL connection is a new fresh session, it will not share any SessionState.
So `USE` state is discarded after every RPC and does not affect 
the next query even on the same ADBC connection.

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

Every request may carry `default-catalog` and/or `default-schema` gRPC
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
