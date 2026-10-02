# datafusion-quack

Serve a [DataFusion](https://datafusion.apache.org) `SessionContext` over DuckDB's
[Quack protocol](https://duckdb.org/docs/current/quack/overview), the way
[datafusion-postgres](https://github.com/datafusion-contrib/datafusion-postgres)
serves pgwire. DuckDB (`ATTACH 'quack:…'`), the Rust
[`quack_protocol`](https://crates.io/crates/quack_protocol) client and the DataFusion
Quack table provider can then all query DataFusion.

```text
DuckDB 2.0 ──ATTACH 'quack:host:9494'──┐
quack_protocol (Rust) ─────────────────┼──▶ datafusion-quack ──▶ SessionContext
DataFusion Quack table provider ───────┘    (HTTP POST /quack)    (CSV, Parquet, …)
```

## Quick start

Serve files with the command-line server:

```sh
cargo install --git https://github.com/smithclay/datafusion-quack datafusion-quack-cli
datafusion-quack --token s3cret-token --parquet lineitem:lineitem.parquet --csv trips:trips.csv
```

and query them from DuckDB 2.0:

```sh
duckdb -c "CREATE SECRET (TYPE quack, TOKEN 's3cret-token');
           ATTACH 'quack:localhost:9494' AS df;
           FROM df.lineitem LIMIT 5;"
```

`SHOW ALL TABLES`, `DESCRIBE df.lineitem`, filters, joins and aggregates all work:
DuckDB pushes whole queries to the server, and the server answers with DuckDB's
result types.

### Embedding

```rust,no_run
use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_quack::{ServerOptions, duckdb_session_config, serve};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new_with_config(duckdb_session_config());
    ctx.register_csv("trips", "trips.csv", CsvReadOptions::new()).await?;
    serve(Arc::new(ctx), &ServerOptions::new().with_token("s3cret-token")).await?;
    Ok(())
}
```

`duckdb_session_config()` puts tables in catalog `memory`, schema `main`, where DuckDB
looks for them. Any `SessionContext` works; with DataFusion's defaults DuckDB finds
the tables at `df.public.trips`.

For more control use `QuackServer`: a custom `AuthProvider` (with a per-query
authorize hook), a `SessionContextProvider` that builds each session's context,
`QueryHook`s that answer statements before DataFusion, TLS, and a shutdown signal.

## Command-line server

```text
datafusion-quack [OPTIONS]
  --host <HOST>            [default: 127.0.0.1]
  -p, --port <PORT>        [default: 9494]
  --token <TOKEN>          token clients must present ($QUACK_TOKEN)
  --tls-cert <PEM>         serve HTTPS (with --tls-key)
  --csv <NAME:PATH>        register a CSV file (repeatable)
  --parquet <NAME:PATH>    register a Parquet file or directory (repeatable)
  --json <NAME:PATH>       register a newline-delimited JSON file (repeatable)
  -d, --dir <DIR>          register every .csv/.parquet/.json/.ndjson file in DIR
  --seed provider-fixtures preload the fixture tables of the provider test suite
  --catalog / --schema     the default catalog and schema [memory / main]
  --memory-limit <BYTES>   memory for queries and unread results, e.g. 4G [unlimited]
  --result-semantics <duckdb|datafusion>
                           whose result types sessions get [by client, see below]
  --metrics-addr <ADDR>    serve Prometheus metrics at http://ADDR/metrics
  --read-only              refuse DDL and DML (CREATE, INSERT, COPY … TO, …) from clients
  --max-sessions, --heartbeat-max, --result-ttl, --inline-rows
```

The command behaves like an in-memory DuckDB with files to read. Tables clients create
(`CREATE TABLE`, `CREATE TABLE … AS`) live in memory, shared by every client, and take
`INSERT`, `UPDATE` and `DELETE`; they are gone when the server stops. Files given on
the command line are read in place, on every query, and never changed: a write to one
is refused with a hint to copy it first, as in DuckDB:

```sql
CREATE TABLE df.my_trips AS SELECT * FROM df.trips;   -- a writable copy in memory
```

Logging is through `tracing`; set `RUST_LOG=datafusion_quack=debug` to see every query.

## Metrics

With the `metrics` feature, the server reports through the
[`metrics`](https://docs.rs/metrics) facade; install any recorder (the CLI's
`--metrics-addr` installs a Prometheus exporter). Without a recorder they cost nothing.

| Metric | Type | Labels |
|---|---|---|
| `quack_requests_total` | counter | `message`: connection, prepare, fetch, cancel, heartbeat, … |
| `quack_errors_total` | counter | `exception`: the DuckDB exception type |
| `quack_sessions` | gauge | |
| `quack_sessions_closed_total` | counter | `reason`: disconnect, expired |
| `quack_statements_total` | counter | `outcome`: ok, error, cancelled |
| `quack_prepare_seconds` | histogram | |
| `quack_results_expired_total` | counter | |
| `quack_result_bytes_held` | gauge | bytes held until the client acknowledges them |
| `quack_response_bytes_total` | counter | |

## Crates

| Crate | What it does |
|---|---|
| [`arrow-quack`](arrow-quack) | Arrow types as DuckDB logical types and type names; `RecordBatch` → DuckDB `DataChunk` encoding |
| [`datafusion-quack-catalog`](datafusion-quack-catalog) | DuckDB catalog emulation: `duckdb_tables()` and friends, `information_schema` with DuckDB type names, DuckDB name resolution, functions and result types |
| [`datafusion-quack`](datafusion-quack) | The server: HTTP, sessions, result cursors, auth, hooks |
| [`datafusion-quack-cli`](datafusion-quack-cli) | The `datafusion-quack` command |

## How it works

Quack is DuckDB's client–server protocol: DuckDB `BinarySerializer` messages over
HTTP `POST /quack`, with results as DuckDB `DataChunk`s. A client opens a session
(CONNECTION) and keeps its lease with HEARTBEATs. A PREPARE runs a statement; the
response carries the first rows, and the client FETCHes numbered batches of the rest,
acknowledging what it has. A retried FETCH gets the same bytes back; DuckDB fetches
several batches ahead at once.

Each session gets its own `SessionContext` from a `SessionContextProvider`. The
default shares the base context's catalogs (a table one client creates is visible to
all) and adds DuckDB compatibility from `datafusion-quack-catalog`:

- **Catalog.** DuckDB's `ATTACH` reads the remote catalog with `duckdb_schemas()`
  (through a recursive CTE) and `duckdb_tables()`, whose `sql` column holds a `CREATE
  TABLE` statement it parses. The table provider reads `information_schema.columns`.
  Both see DuckDB type names, and names match case-insensitively, as in DuckDB.
- **Functions.** DuckDB sends some expressions back as calls to its own system
  functions (`"system".main."add"(…)`, `to_days(…)`, `count_star()`); these resolve.
- **Result types, for DuckDB clients.** DuckDB shows the server's results as they come,
  so a DuckDB client's session gets DuckDB's types where they differ from
  DataFusion's: `/` on integers and decimals is `DOUBLE`, `avg(DECIMAL)` is `DOUBLE`,
  `DATE + INTERVAL` is a `TIMESTAMP`. Other clients, which report no DuckDB version,
  keep DataFusion's semantics. `ServerOptions::with_result_semantics` (CLI:
  `--result-semantics`) gives every session one or the other instead. In every
  session, `sum` of integers is `DECIMAL(38,0)` (DuckDB: `HUGEINT`), so it never
  wraps.

Clients may run any statement DataFusion supports, as with DuckDB's own server. For
untrusted clients, make the server read-only with `ServerOptions::with_sql_options`
(CLI: `--read-only`): DDL includes `CREATE EXTERNAL TABLE` over the server's files, and
DML includes `COPY … TO`, which writes them.

DataFusion has no transactions: each statement takes effect as it runs. `BEGIN` and
`COMMIT` are accepted, and `ROLLBACK` succeeds when nothing since `BEGIN` could have
changed data. After a write it fails with a `TransactionContext` error saying the
writes were kept, rather than reporting a rollback that didn't happen. DDL and DML run
where DataFusion supports them, e.g. on `MemTable`s.

## Compatibility

Tested against DuckDB `v2.0.0-alpha43586` with the quack extension `974927a394`
(Quack protocol v3):

- A DuckDB `ATTACH` differential test runs TPC-H SF0.01 (all 22 queries) and a type
  matrix through the server and natively in DuckDB, and requires identical results.
- For the same table and requests, the server's responses are byte-identical to
  DuckDB's `quack_serve` (`testdata/wire/golden`).
- The `datafusion-table-providers` Quack suite passes in its seeded mode, and the
  read-only part of the `quack_protocol` live suite passes.

Types DuckDB has and DataFusion lacks (HUGEINT, UUID, ENUM, UNION, TIMETZ, VARIANT,
GEOMETRY, collations) can't be served. Arrow types without a DuckDB counterpart
(Decimal256, unions, run-end encoding) are an error when a query returns them. Error
messages come from DataFusion, so their text differs from DuckDB's. Protocol v1
clients (DuckDB 1.x) and client-to-server data (`SEND_DATA`) are not supported.

## Testing

```sh
cargo test --workspace                 # unit, property, replay, wire and hostile-input tests
tests-integration/run.sh               # real DuckDB 2.0, the quack_protocol and provider suites
cd fuzz && cargo +nightly fuzz run dispatch
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the quality gates and review norms.

## License

Apache-2.0
