# Server

The optional `[server]` table configures the DataFusion query engine runtime.

```toml
[server]
memory-limit = "4GB"
flight-sql-server-port = 32010
web-ui-port = 6060
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `memory-limit` | String | No | Unlimited | Caps the memory available to the query engine. |
| `flight-sql-server-port` | Integer | No | 32010 | Port the Arrow Flight SQL server started by `--flight-sql-server` listens on. |
| `web-ui-port` | Integer | No | 6060 | Port the web UI started by `--ui` listens on. |

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

Currently supported: statement execution (`CommandStatementQuery`) with
streaming Arrow results. Prepared statements and catalog metadata commands
(`GetCatalogs`, `GetTables`, ...) are not implemented yet. Each request runs in
its own session, so `USE` statements do not carry over between queries — use
fully qualified table names (`<catalog>.<schema>.<table>`).

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

## Web UI

Lakelet can serve a browser-based web UI:

```bash
lakelet --config config.toml --ui
```

The server listens on `web-ui-port` under `[server]` (default 6060) and
prints where to open it:

```text
Lakelet is running:
  Web UI: http://127.0.0.1:6060
Press Ctrl+C to stop.
```

Each query runs in its own session, so `USE` does not carry over between
queries — use fully qualified table names (`<catalog>.<schema>.<table>`).
