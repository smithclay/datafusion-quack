# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); release-plz writes the
entries for each release.

## [Unreleased]

## [0.1.0](https://github.com/smithclay/datafusion-quack/releases/tag/arrow-quack-v0.1.0) - 2026-10-02

### Added

- serve DataFusion over DuckDB's Quack protocol ([#1](https://github.com/smithclay/datafusion-quack/pull/1))

### Added

- `datafusion-quack`: a Quack protocol v3 server for a DataFusion `SessionContext`:
  sessions with heartbeat leases and a reaper, result cursors with read-ahead FETCH,
  acknowledgements and cancel, token auth with an authorize hook, query hooks, TLS.
- `arrow-quack`: Arrow to DuckDB logical types, type names and `DataChunk` encoding.
- `datafusion-quack-catalog`: DuckDB catalog functions, `information_schema` with
  DuckDB type names, DuckDB name resolution, functions and result types.
- `datafusion-quack-cli`: the `datafusion-quack` command, serving CSV, Parquet and
  JSON files.
