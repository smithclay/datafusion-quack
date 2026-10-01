# datafusion-quack-catalog

DuckDB catalog emulation for DataFusion, part of
[datafusion-quack](https://github.com/smithclay/datafusion-quack).

`duckdb_session_state(state)` makes a DataFusion session answer DuckDB clients'
catalog SQL: `duckdb_tables()` and friends, `information_schema.columns` with DuckDB
type names, case-insensitive names, and the functions DuckDB sends
(`current_database()`, `count_star()`, `"system".main.add(…)`, `to_days(…)`, …).
`duckdb_client_semantics(state)` adds DuckDB's result types for sessions of DuckDB
clients.
