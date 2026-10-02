# arrow-quack

Arrow to DuckDB `DataChunk` encoding for the
[Quack protocol](https://duckdb.org/docs/current/quack/overview), part of
[datafusion-quack](https://github.com/smithclay/datafusion-quack).

- `arrow_to_logical_type(&DataType)`: the DuckDB logical type values are sent as.
- `duckdb_type_name(&Field)`: the DuckDB name of a type, e.g. `DECIMAL(10,2)`,
  `TIMESTAMP WITH TIME ZONE`, `STRUCT(a INTEGER, "b c" VARCHAR)`.
- `encode_record_batch(&RecordBatch)`: `DataChunk`s of at most 2048 rows, written
  column by column from the Arrow buffers, byte-identical to what DuckDB 2.0 writes.

Types without a DuckDB counterpart are an `Error::Unsupported`, never a panic.
