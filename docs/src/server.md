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
| `flight-sql-server-port` | Integer | Required for `--flight-sql-server` | None | Port the Arrow Flight SQL server listens on. |
| `web-ui-port` | Integer | Required for `--web-ui` | None | Port the web UI HTTP API listens on. Binds `127.0.0.1` only. |

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

## Web UI API server

Lakelet can serve an HTTP API for the browser-based web UI:

```bash
lakelet --config config.toml --web-ui
```

`web-ui-port` must be configured under `[server]`, otherwise startup fails.
The server binds `127.0.0.1` only and has no authentication; any-origin CORS
(including Chrome's Private Network Access preflight) is enabled so the
separately hosted UI can reach it cross-origin.

The binary contains no UI assets. The UI lives in `web-ui/` in this
repository: run it locally with `npm install && npm run dev`, or build with
`npm run build` and host the generated `web-ui/dist/` on any static host or
CDN. When opened, the page asks for the local port (pre-filled with 6060) and
connects to `http://127.0.0.1:<web-ui-port>`.

Endpoints: `POST /api/query` (JSON in, streaming Arrow IPC out) and
`GET /api/info`. See the module docs in `src/app/src/server/web/mod.rs` for
the full request/response reference. Each request runs in its own session,
so `USE` does not carry over — pass `catalog`/`schema` in the request body or
use fully qualified table names.
