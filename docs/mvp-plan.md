# datafusion-quack — MVP plan

## Context

`datafusion-table-providers` (branch `feat/quack-provider-v2`) can now *read from* a Quack server
(DuckDB's client–server protocol, v3) using `quack_protocol` 0.3 from bnjjj/quack_protocol_rs.
`datafusion-quack` is the reverse: it lets any DataFusion `SessionContext` *serve* the Quack protocol,
the same way `datafusion-contrib/datafusion-postgres` serves pgwire. Then DuckDB clients
(`ATTACH 'quack:…'`), the Rust `quack_protocol` client and the DataFusion Quack provider can all query
DataFusion.

### What the research found (summary)

- **Protocol.** HTTP `POST /quack`. The body is DuckDB `BinarySerializer` messages (field-id tagged, ULEB),
  and results are DuckDB `DataChunk`s, not Arrow.
  - Messages: CONNECTION, PREPARE (with inline rows), FETCH (batch_index/ack_index), HEARTBEAT, DISCONNECT,
    CANCEL, ERROR.
  - Errors are always returned as HTTP 200 with an `ErrorResponse` body. `GET /` returns a 200 banner.
  - There is no published spec. The authority is `duckdb/duckdb-quack@main`: `src/include/quack_message.json`
    and `src/quack_server.cpp`.
- **Codec.** `quack_protocol` 0.3.0 already contains the encode/decode for every server-side message, but it
  is all `pub(crate)`. It has no server, no Arrow→DataChunk path, and no encoder for the v2 string layout or
  for ErrorResponse fields 2–4. `Cargo.toml` says `license = "MIT"`, but the repo has no LICENSE file.
- **Clients we must satisfy:**
  1. The `quack_protocol` Rust client: v3 only from the provider, heartbeats every timeout/3, strict FETCH
     indexing rules.
  2. The provider's SQL: `information_schema.{columns,tables,schemata}` with DuckDB-style `data_type`
     strings and `YES`/`NO` nullability, `current_database()`/`current_schema()`, and DuckDB-dialect
     SELECTs from the unparser, including whole federated join and aggregate subplans.
  3. DuckDB 2.0 `ATTACH`:
     - `WITH RECURSIVE … duckdb_schemas() … list_append`
     - `duckdb_tables()`/`duckdb_views()` returning `CREATE TABLE` SQL
     - `BEGIN`/`COMMIT`/`ROLLBACK`
     - pushdown scans from `quack_scan.cpp`
- **Prior art.**
  - `schubergphilis/sqe` (Apache-2.0) has a DF-55 Quack server and wire crate. It is worth reading for its
    session store and its fuzzing. Its own review notes flag two problems: hostile input panicking the Arrow
    bridge, and sessions with no expiry or result cap.
  - `datafusion-postgres` and `datafusion-flight-sql-server` are the structural templates.

## Decisions (confirmed)

1. **Codec.** Upstream a `server` feature to `quack_protocol`. Until it lands, depend on a git rev of the
   `smithclay` fork. The Arrow↔DataChunk work goes in our own `arrow-quack` crate, the same way
   datafusion-postgres has `arrow-pg`.
2. **MVP clients.** The Rust client, the provider, **and DuckDB `ATTACH`**. That means the MVP includes
   DuckDB catalog emulation, the equivalent of `datafusion-pg-catalog`.
3. **Provider interop gate.** A server-seeded fixture mode: the provider tests skip their DuckDB-only DDL
   and types and run against tables our server preloads.

## Architecture (workspace layout, mirrors datafusion-postgres)

```
datafusion-quack/
  Cargo.toml                  # [workspace], resolver 2, edition 2024, rust-version pinned, Cargo.lock committed
  arrow-quack/                # Arrow ⇄ DuckDB LogicalType; RecordBatch → DataChunk encoder; DuckDB type-name rendering
  datafusion-quack-catalog/   # duckdb_schemas/tables/views/columns UDTFs, information_schema overlay,
                              #   current_database()/current_schema(), DuckDB→DF SQL rewrites
  datafusion-quack/           # library: HTTP server, sessions, result cursors, auth, hooks, serve()
  datafusion-quack-cli/       # binary: serve CSV/Parquet/JSON files, --token, --seed fixtures
  tests-integration/          # scripts that drive real DuckDB 2.0 CLI + Rust client + provider suite
  testdata/wire/              # golden byte fixtures captured from real quack_serve
```

### `arrow-quack`
- `arrow_to_logical_type(&DataType) -> Result<LogicalType>` and `duckdb_type_name(&Field) -> String`.
  The second returns names like `DECIMAL(10,2)`, `TIMESTAMP_MS` and `TIMESTAMP WITH TIME ZONE`. It is used
  by both information_schema and the `CREATE TABLE` SQL.
- `encode_record_batch(&RecordBatch) -> Result<Vec<DataChunk>>`. It splits batches at 2048 rows
  (STANDARD_VECTOR_SIZE) and handles sliced arrays (respect `offset()`, which was a datafusion-postgres
  #419 bug).
- MVP type set:
  - bool, int8–64, uint8–64, float32/64, decimal128, utf8/large_utf8/utf8view, binary variants
  - date32, time64(us), timestamp(s/ms/us/ns, ±tz), interval
  - list, struct, fixed-size list
- Anything else returns a typed `Unsupported` error, never a panic.
- Depends on `quack_protocol` with **its `arrow` feature off**, so its arrow 58 never meets our DataFusion's
  arrow 59.

### `datafusion-quack-catalog`
- Table functions `duckdb_schemas()`, `duckdb_tables()` (with a `sql` column containing the generated
  `CREATE TABLE`), `duckdb_views()` and `duckdb_columns()`. They are built from the `SessionContext` catalog
  list, with stable oids per session.
- An overlay for `information_schema.columns` that renders `data_type` as DuckDB names and `is_nullable` as
  `YES`/`NO`. Plus scalar UDFs `current_database()` and `current_schema()`.
- SQL is parsed with DataFusion's `duckdb` dialect (`datafusion.sql_parser.dialect = 'duckdb'`). Remaining
  gaps such as `list_append` and recursive CTE shapes are closed with UDFs or planner extensions first.
  AST rewrites are only for catalog queries. This is the maintainer guidance from datafusion-postgres
  #419.
- Every catalog query a real client sends is captured verbatim into replay tests (see Gate G3).

### `datafusion-quack` (library)
- API, matching datafusion-postgres and flight-sql-server:
  - `serve(Arc<SessionContext>, &ServerOptions)`
  - `serve_with_hooks(…, Vec<Arc<dyn QueryHook>>)`
  - `serve_with_listener(…)` for tests and embedding
  - `ServerOptions` builder: host, port (default 9494), token, TLS cert/key, `max_sessions`,
    `heartbeat_max`, `result_ttl`, `inline_rows`
  - Defaults are "off/0 = unlimited" except safety limits, which are documented.
- `AuthProvider` trait. The default is a constant-time token compare against `auth_string`, with an
  optional per-query authorize hook (DuckDB has one). `SessionContextProvider` makes a per-session
  `SessionContext`, the flight-sql-server pattern.
- HTTP uses axum/hyper: `GET /` returns the banner, `OPTIONS /quack` returns 204 with CORS headers, and
  `POST /quack` is the dispatcher. It always returns 200, with errors encoded as `ErrorResponse`. There is
  a body size limit.
- Sessions:
  - `connection_id` is a random 128-bit value with a heartbeat lease, swept by a reaper task.
  - There is one active cursor per session. A new PREPARE cancels the old stream.
  - `query_uuid` must match on FETCH.
  - Batch numbering starts at 1, after the inline batches. Batches are kept until acked so a retried FETCH
    returns identical bytes.
  - The end of a stream is an empty FETCH_RESPONSE carrying `total_batches`.
  - CANCEL (uuid 0 means "whatever is running") drops the stream.
- Transactions: `BEGIN`/`COMMIT`/`ROLLBACK` are accepted as no-ops through a built-in `QueryHook`
  (datafusion-postgres has the same `hooks/transactions.rs`). The MVP is read-only, except DDL/DML that
  DataFusion handles natively on MemTables.
- Logging uses `tracing` only. Nothing in the library prints.

### `datafusion-quack-cli`
`datafusion-quack --token T --port 9494 --csv name:path --parquet name:path -d dir --seed provider-fixtures`

## Milestones

| # | Deliverable | Exit criteria |
|---|---|---|
| M0 | Repo skeleton, CI baseline (G1), LICENSE (Apache-2.0), README stub. Fork `quack_protocol` and open the upstream `server`-feature PR. | CI green on an empty workspace. Upstream PR opened. |
| M1 | Upstream feature: make `QuackMessage` plus encode/decode public for server-received and server-sent messages, add an ErrorResponse 2–4 encoder, and ask the maintainer to add a LICENSE file. | Byte round-trip tests pass against golden fixtures captured from DuckDB 2.0 `quack_serve`. |
| M2 | `arrow-quack` encoder plus type-name rendering. | G4 property tests: Arrow → DataChunk → `quack_protocol` decode → Arrow is identity for the MVP type set, sliced arrays included. |
| M3 | Server core: handshake, PREPARE with inline rows, FETCH and ack, heartbeat, disconnect, cancel, auth, session reaper. | The `quack_protocol` client (v3) runs `SELECT` over 1M rows across many batches, gets a correct error for bad SQL, rejects a bad token, and keeps a session alive past its heartbeat timeout. |
| M4 | Catalog emulation plus DuckDB `ATTACH`. | DuckDB 2.0 CLI: `ATTACH 'quack:127.0.0.1:P' AS df; SHOW ALL TABLES; DESCRIBE df.t; FROM df.t WHERE …;` all succeed with correct results. |
| M5 | Provider interop plus seeded mode, CLI, docs, first release. | Gates G5 and G6 green. `v0.1.0` released by release-plz. |

## MVP success criteria (definition of done)

1. **Rust client.** The `quack_protocol` 0.3 client's live test suite (`tests/integration.rs`), limited to
   read-only tests, passes against `datafusion-quack-cli`.
2. **Provider.** The `datafusion-table-providers` quack suite passes in seeded mode for every test that does
   not need DuckDB-only types. That covers:
   - type round trip for the MVP type set
   - nullability from the catalog
   - exact filter pushdown
   - limit, projection, count and sort
   - the three federation tests
   - pool exhaustion and dropped streams
   - name resolution
   - `CREATE EXTERNAL TABLE`
3. **DuckDB.** A stock DuckDB 2.0 CLI can `ATTACH` our server, list schemas and tables, `DESCRIBE`, and run
   filtered, projected and aggregated queries.
   - A **differential test** loads the same Parquet into native DuckDB and into our server, runs a query set
     through `ATTACH` (TPC-H SF0.01 plus a type-matrix table), and requires identical results.
4. **Robustness.** No panic on any malformed request body (fuzzed). Sessions expire. Result memory is bounded
   by `max_sessions × in-flight batches`.
5. **Ergonomics.** About 10 lines of embedding code: `serve(ctx, &ServerOptions::new().with_token(..))`. The
   README quick start works copy-pasted.

## Quality gates

These are drawn from datafusion-postgres, flight-sql-server, roapi and datafusion-contrib norms, plus the
maintainer review themes.

**G1 — required on every PR.** A single `ci.yml`, with `Swatinem/rust-cache` and `dtolnay/rust-toolchain`:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`, plus a second run with `--all-features`.
  Both are required; optional features that lag upstream may be `continue-on-error`, as in
  datafusion-postgres #411.
- `cargo test --workspace --locked`, on a stable plus nightly matrix and a per-feature matrix
- An MSRV job: `cargo build` on the `rust-version` declared in `Cargo.toml`, kept equal. datafusion-postgres
  has these drifted apart (1.89 vs 1.94); we won't.
- `cargo doc --no-deps` with `RUSTDOCFLAGS=-D warnings`
- `cargo package` dry-run for each publishable crate (flight-sql-server does this)
- `cargo deny check` for licenses, bans and advisories. This is what catches the missing LICENSE on the
  MIT dependency.
- `typos`

**G2 — wire conformance.** Golden byte fixtures in `testdata/wire/`, captured from DuckDB 2.0 `quack_serve`
with a recording proxy script. Our encoder must produce byte-identical messages for the same logical
response, or the documented differences must be justified.

**G3 — per-client SQL replay** (the datafusion-postgres `tests/dbeaver.rs` pattern). The proxy records every
PREPARE SQL from:
- the DuckDB 2.0 `ATTACH` flow
- the provider flow
- the `quack_protocol` client

These are stored as `const DUCKDB_ATTACH_QUERIES: &[&str]` and replayed in-process. Every query must plan
and execute.

**G4 — property and fuzz.**
- `proptest` round-trips for `arrow-quack`.
- A `cargo-fuzz` target on request decoding and dispatch, run for N minutes nightly. A short smoke run
  happens per PR.
- Explicit hostile-input tests: truncated ULEB, huge lengths, unknown field ids, wrong `connection_id`.

**G5 — real-client integration, required.** `tests-integration/` jobs:
1. Download the pinned DuckDB 2.0 CLI and run the `ATTACH` scenario plus the differential TPC-H test.
2. Run the `quack_protocol` live suite against our CLI.
3. Check out `smithclay/datafusion-table-providers@<pinned sha>` and run
   `QUACK_SERVER_URI=… QUACK_SEED_MODE=1 cargo test -p datafusion-table-providers --test integration --features quack -- quack`.

Server logs are uploaded as artifacts on failure.

**G6 — release hygiene.**
- release-plz, with independent crate versions and tags `<crate>-vX`.
- `CHANGELOG.md`, generated by release-plz, which avoids datafusion-postgres' duplicated release notes.
- Dependabot: daily for cargo, weekly for actions.
- Nightly cron CI run to catch upstream breakage.

**Review norms** (taken from maintainer feedback; they go in `CONTRIBUTING.md`):
- Extend through hooks and planners, not ad-hoc SQL rewrites (dfpg #419, #204).
- Feature-gate optional things and give each its own CI matrix entry (#419).
- Small, focused PRs. Commit a failing regression test first (#150, #392).
- Errors: typed, never string-matched, never swallowed (#126, #419). No `unwrap` or `expect` on input-derived
  data. Clippy `unwrap_used` is denied in the library crates.
- No `println!` in libraries (#98). Use rustls with the `ring` provider for Windows portability (#98).
- Keep the public API stable. Upgrade DataFusion with a manual PR that moves arrow and DataFusion together;
  close dependabot's DataFusion major bumps (#409, #321).
- Track the current DataFusion major (55 / arrow 59), with `default-features = false` on workspace deps.
  `Cargo.lock` is committed and CI builds with `--locked` (flight-sql #68).

## Risks

- **The upstream PR may be slow or rejected.** Fallback: keep the fork's git rev, or vendor the codec into a
  `quack-wire` crate. `sqe-quack-wire` (Apache-2.0) is a reference.
- **Protocol churn before DuckDB 2.0 is final.** Pin the DuckDB build in CI. Golden fixtures make drift
  visible.
- **DuckDB `ATTACH` SQL surface.** Unknown until captured. M4 starts with the capture step, and the scope is
  sized from the transcript. A recursive CTE plus `list_append` may need a DataFusion feature or UDF.
- **Perf.** `DataChunk` in `quack_protocol` is row-oriented `Vec<Value>`. Fine for the MVP; a columnar
  encoder in `arrow-quack` comes after the MVP, with a criterion bench added in M2 to track it.
- **Contrib transfer.** The process is informal: ask on DataFusion Slack/Discord or in an issue once M5
  ships, then re-point crates.io trusted publishing after the transfer (tpcgen-rs #404).

## Verification

- Local: `cargo test --workspace` (G1–G4), then `tests-integration/run.sh`, which runs G5 locally with the
  pinned DuckDB CLI.
- Manual smoke test:
  ```
  datafusion-quack --token t --parquet lineitem:…
  duckdb -c "CREATE SECRET (TYPE quack, TOKEN 't'); ATTACH 'quack:localhost:9494' AS df; FROM df.lineitem LIMIT 5;"
  ```
