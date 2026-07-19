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
| `flight-sql-server-port` | Integer | Required for `--flight-sql-server` | None | Port the Arrow Flight SQL server listens on. |

The value is an integer with an optional, case-insensitive unit. With no unit, the value is treated as bytes.

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

`flight-sql-server-port` must be configured under `[server]`, otherwise startup
fails. The server binds to `0.0.0.0` and accepts remote connections. Note that
the server currently has **no authentication**, so only expose it on networks
you trust.

Currently supported: statement execution (`CommandStatementQuery`) with
streaming Arrow results. Prepared statements and catalog metadata commands
(`GetCatalogs`, `GetTables`, ...) are not implemented yet. Each request runs in
its own session, so `USE` statements do not carry over between queries — use
fully qualified table names (`<catalog>.<schema>.<table>`) or set
`--default-catalog` / `--default-schema` at startup.

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
