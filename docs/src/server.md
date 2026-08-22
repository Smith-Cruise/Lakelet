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
| `memory-limit` | String | No | Unlimited | Caps the memory available to the query engine. |
| `flight-sql-server-port` | Integer | No | 32010 | Port the Arrow Flight SQL server started by `--flight-sql-server` listens on. |

The `memory-limit` value is an integer with an optional, case-insensitive unit. With no unit, the value is treated as bytes.

| Unit | Meaning |
| --- | --- |
| `b` | Bytes |
| `k`, `kb` | Kibibytes |
| `m`, `mb` | Mebibytes |
| `g`, `gb` | Gibibytes |
| `t`, `tb` | Tebibytes |

## Arrow Flight SQL server

Lakelet can run as an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
server instead of the interactive REPL:

```bash
lakelet --config config.toml --flight-sql-server
```

The server listens on `flight-sql-server-port` under `[server]` (default
32010).

Currently supported: statement execution (`CommandStatementQuery`), prepared
statements (without parameter binding), and `GetSqlInfo`, all with streaming
Arrow results. Catalog metadata commands (`GetCatalogs`, `GetTables`, ...) are
not implemented yet, so ADBC's `adbc_get_objects()` and catalog browsing in
SQL tools do not work. Each request runs in its own session, so `USE`
statements do not carry over between queries — either use fully qualified
table names (`<catalog>.<schema>.<table>`), or set the default per request
with the `default-catalog` and `default-schema` gRPC headers (see below).

### Default catalog and schema headers

Every request may carry `default-catalog` and/or `default-schema` gRPC
metadata headers; they override the server-wide defaults for that request, so
unqualified table names resolve against them. The header names match the
`--default-catalog`/`--default-schema` CLI flags.

With ADBC, pass them as call headers — set at connection level they are
attached to every RPC automatically:

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
) as conn:
    ...
```

With the Arrow Flight SQL JDBC driver, URL parameters the driver does not
recognize are forwarded as gRPC headers, so
`?default-catalog=hive&default-schema=sales` works. Do not use the driver's
own `catalog=` parameter: the JDBC driver intercepts it and issues a
`SetSessionOptions` action, which Lakelet does not implement.

### Connect with ADBC (Python)

Lakelet works with the [ADBC](https://arrow.apache.org/adbc/) Flight SQL
driver:

```bash
pip install adbc_driver_flightsql pyarrow
```

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://127.0.0.1:32010") as conn:
    with conn.cursor() as cur:
        cur.execute("select 1 as a")
        print(cur.fetch_arrow_table())
```

Parameter binding (`cur.execute(sql, params)`) is not supported.

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
