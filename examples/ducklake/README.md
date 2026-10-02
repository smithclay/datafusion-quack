# Serving a DuckLake catalog

Serves a [DuckLake](https://ducklake.select) catalog to DuckDB over Quack, with
[datafusion-ducklake](https://github.com/datafusion-contrib/datafusion-ducklake) reading
the catalog and its Parquet files. DuckDB attaches the server as it would any Quack
server, and its queries run in DataFusion.

```text
DuckDB 2.0 ──ATTACH 'quack:…'──▶ ducklake-quack ──▶ DuckLakeCatalog ──▶ metadata.sqlite
                                 (datafusion-quack)   (datafusion-ducklake)  + Parquet files
```

This crate is not part of the workspace: it builds datafusion-ducklake from a checkout
next to this repository (`../datafusion-ducklake`).

## 1. Install DuckDB 2.0

Quack needs DuckDB 2.0, which is in alpha:

```sh
curl https://install.duckdb.org | DUCKDB_VERSION=alpha sh   # installs ~/.duckdb/cli/latest/duckdb
```

## 2. Make a catalog

```sh
mkdir -p lake/data
~/.duckdb/cli/latest/duckdb -c "
  ATTACH 'ducklake:sqlite:lake/meta.sqlite' AS lake (DATA_PATH 'lake/data/', DATA_INLINING_ROW_LIMIT 0);
  USE lake;
  CREATE TABLE trips (id INTEGER, city VARCHAR, fare DECIMAL(10,2), ts TIMESTAMP, tags VARCHAR[]);
  INSERT INTO trips SELECT i, ['nyc','sf','la'][i%3+1], (i*1.25)::DECIMAL(10,2),
                           TIMESTAMP '2026-01-01' + INTERVAL (i) HOUR, ['a','b'] FROM range(1000) t(i);
  DELETE FROM trips WHERE id % 10 = 0;
  CREATE SCHEMA ops;
  CREATE TABLE ops.cities (city VARCHAR, state VARCHAR);
  INSERT INTO ops.cities VALUES ('nyc','NY'), ('sf','CA'), ('la','CA');
  CREATE VIEW nyc_trips AS SELECT * FROM trips WHERE city = 'nyc';"
```

`DATA_INLINING_ROW_LIMIT 0` keeps every row in Parquet. DuckDB 2.0 otherwise stores small
inserts in the catalog itself, in tables whose columns (`_ducklake_row_id`,
`_ducklake_begin_snapshot`, …) datafusion-ducklake does not read yet, so queries of
those tables fail with `no such column: begin_snapshot`.

## 3. Serve it

```sh
cargo run --release -- lake/meta.sqlite --token s3cret-token     # add --write to allow INSERT, CREATE TABLE, …
```

## 4. Query it

```sh
~/.duckdb/cli/latest/duckdb -c "
  CREATE SECRET (TYPE quack, TOKEN 's3cret-token');
  ATTACH 'quack:localhost:9494' AS lake;
  SHOW ALL TABLES;
  SELECT c.state, count(*), avg(t.fare)
  FROM lake.main.trips t JOIN lake.ops.cities c USING (city) GROUP BY ALL;
  FROM lake.main.nyc_trips LIMIT 5;"
```

The catalog is registered as `memory`, DuckDB's default database name, so the tables
are at `lake.<schema>.<table>`. The server reads the latest snapshot for each query, so
it sees what other writers commit while it runs.

## Listing the catalog

DuckDB's `ATTACH` asks for every table's columns. Opening a DuckLake table reads its
columns, statistics and partitioning, so describing the catalog table by table costs a
dozen metadata queries per table. `src/listing.rs` gives quack a `CatalogListing` that
reads every table's columns in a few bulk queries instead: on a 200-table catalog,
`ATTACH` went from 5,293 metadata queries to 85.

## Building on macOS 27

`Cargo.toml` sets `strip = false` for build dependencies: a stripped proc-macro dylib
built by Rust before 1.98.1 has a misaligned string pool, which macOS 27's dyld refuses
(`mis-aligned LINKEDIT string pool`), failing the build at `sqlx`.
