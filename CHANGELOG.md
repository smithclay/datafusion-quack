# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); release-plz writes the
entries for each release.

## [Unreleased]

## [0.1.0](https://github.com/smithclay/datafusion-quack/releases/tag/datafusion-quack-cli-v0.1.0) - 2026-10-09

### Added

- serve DataFusion over DuckDB's Quack protocol ([#1](https://github.com/smithclay/datafusion-quack/pull/1))

## [0.1.0](https://github.com/smithclay/datafusion-quack/releases/tag/datafusion-quack-v0.1.0) - 2026-10-09

### Added

- serve DataFusion over DuckDB's Quack protocol ([#1](https://github.com/smithclay/datafusion-quack/pull/1))

### Other

- *(deps)* Bump reqwest from 0.12.28 to 0.13.5 ([#4](https://github.com/smithclay/datafusion-quack/pull/4))

## [0.1.0](https://github.com/smithclay/datafusion-quack/releases/tag/datafusion-quack-catalog-v0.1.0) - 2026-10-09

### Added

- serve a DuckLake catalog; list catalogs for ATTACH without opening each table ([#5](https://github.com/smithclay/datafusion-quack/pull/5))
- serve DataFusion over DuckDB's Quack protocol ([#1](https://github.com/smithclay/datafusion-quack/pull/1))

## [0.1.0](https://github.com/smithclay/datafusion-quack/releases/tag/arrow-quack-v0.1.0) - 2026-10-09

### Added

- serve a DuckLake catalog; list catalogs for ATTACH without opening each table ([#5](https://github.com/smithclay/datafusion-quack/pull/5))
- serve DataFusion over DuckDB's Quack protocol ([#1](https://github.com/smithclay/datafusion-quack/pull/1))

### Other

- *(deps)* Bump criterion from 0.7.0 to 0.8.2 ([#2](https://github.com/smithclay/datafusion-quack/pull/2))

### Added

- `datafusion-quack`: a Quack protocol v3 server for a DataFusion `SessionContext`:
  sessions with heartbeat leases and a reaper, result cursors with read-ahead FETCH,
  acknowledgements and cancel, token auth with an authorize hook, query hooks, TLS.
- `arrow-quack`: Arrow to DuckDB logical types, type names and `DataChunk` encoding.
- `datafusion-quack-catalog`: DuckDB catalog functions, `information_schema` with
  DuckDB type names, DuckDB name resolution, functions and result types.
- `datafusion-quack-cli`: the `datafusion-quack` command, serving CSV, Parquet and
  JSON files.
- `datafusion-quack-catalog`: `CatalogListing`, so `ATTACH` lists a catalog's tables
  without opening each one.
- `examples/ducklake`: serving a DuckLake catalog to DuckDB.

### Fixed

- `datafusion-quack-catalog`: DuckDB's `information_schema` replaces a catalog's own.
- `arrow-quack`: names that are DuckDB `type_function` keywords (`inner`, `left`, …) are
  quoted, so a column or struct field with one no longer breaks `ATTACH`.
