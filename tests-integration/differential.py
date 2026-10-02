#!/usr/bin/env python3
"""Differential test: the same queries on native DuckDB and on datafusion-quack.

Both sides read the same Parquet files. The native side loads them as DuckDB
views; the remote side ATTACHes a datafusion-quack server that serves them. Every
query must return the same rows (floats to a relative tolerance; row order only
where the query orders).

    differential.py --duckdb duckdb --uri quack:127.0.0.1:9494 --token T \
        --data DIR [--queries tpch,types]
"""

import argparse
import decimal
import json
import math
import os
import re
import sys

from duckdb_cli import run as run_duckdb

REL_TOLERANCE = 1e-9

TYPE_QUERIES = [
    "SELECT * FROM type_matrix ORDER BY id",
    "SELECT id, c_dec, c_date, c_ts FROM type_matrix WHERE c_i32 > 0 ORDER BY id",
    "SELECT count(*), sum(c_i64), min(c_f64), max(c_varchar), avg(c_dec) FROM type_matrix",
    "SELECT c_bool, count(*) FROM type_matrix GROUP BY c_bool ORDER BY c_bool",
    "SELECT id, c_list, c_struct FROM type_matrix WHERE c_list IS NOT NULL ORDER BY id",
    "SELECT id FROM type_matrix WHERE c_varchar LIKE 'b%' ORDER BY id",
    "SELECT id, c_date + INTERVAL 1 DAY AS next_day FROM type_matrix ORDER BY id",
    "SELECT id, c_ts::DATE AS d FROM type_matrix ORDER BY id",
    "SELECT id, length(c_varchar), upper(c_varchar) FROM type_matrix ORDER BY id",
    "SELECT max(c_ts) - min(c_ts) AS span FROM type_matrix",
]

TYPE_MATRIX_SQL = """
COPY (
  SELECT
    i AS id,
    (i % 2 = 0) AS c_bool,
    (i * 3 - 7)::TINYINT AS c_i8,
    (i * 100 - 3)::SMALLINT AS c_i16,
    (i * 100000 - 500000)::INTEGER AS c_i32,
    (i * 10000000000 - 3)::BIGINT AS c_i64,
    i::UTINYINT AS c_u8,
    (i * 1000)::UINTEGER AS c_u32,
    (i * 1.5)::FLOAT AS c_f32,
    (i / 7.0)::DOUBLE AS c_f64,
    (i * 12.34)::DECIMAL(12, 2) AS c_dec,
    (i * 1234567890.123)::DECIMAL(38, 3) AS c_dec38,
    CASE WHEN i % 5 = 0 THEN NULL ELSE ['a', 'b', 'c', 'héllo'][i % 4 + 1] || i END AS c_varchar,
    ('blob' || i)::BLOB AS c_blob,
    DATE '2024-01-01' + i::INTEGER AS c_date,
    TIMESTAMP '2024-01-01 12:34:56.789' + INTERVAL (i) HOUR AS c_ts,
    TIME '01:02:03' + INTERVAL (i) MINUTE AS c_time,
    CASE WHEN i % 3 = 0 THEN NULL ELSE [i, i + 1, NULL] END AS c_list,
    {'a': i, 'b': 'x' || i} AS c_struct
  FROM range(1, 41) t(i)
) TO '{path}' (FORMAT parquet);
"""


NUMBER = re.compile(r"^-?\d+(\.\d+)?$")


def normalize(value):
    """DuckDB's JSON output prints HUGEINT and DECIMAL as strings and other numbers as
    numbers, so numbers are compared by value, whatever their type."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return decimal.Decimal(value)
    if isinstance(value, str) and NUMBER.match(value):
        return decimal.Decimal(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        return value
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    return value


def close(a, b):
    if isinstance(a, float) or isinstance(b, float):
        if not isinstance(a, (float, decimal.Decimal)) or not isinstance(b, (float, decimal.Decimal)):
            return False
        return math.isclose(float(a), float(b), rel_tol=REL_TOLERANCE, abs_tol=1e-12)
    if isinstance(a, decimal.Decimal) and isinstance(b, decimal.Decimal):
        return a.compare(b) == 0
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(close(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(close(a[k], b[k]) for k in a)
    return a == b


def rows_equal(native, remote, ordered):
    native = [list(normalize(row).values()) for row in native]
    remote = [list(normalize(row).values()) for row in remote]
    if not ordered:
        key = lambda row: json.dumps(row, sort_keys=True, default=str)
        native, remote = sorted(native, key=key), sorted(remote, key=key)
    return len(native) == len(remote) and all(close(a, b) for a, b in zip(native, remote))


def tpch_queries(duckdb):
    rows = run_duckdb(duckdb, "LOAD tpch; SELECT query_nr, query FROM tpch_queries() ORDER BY 1")
    return [(f"tpch-q{row['query_nr']:02d}", row["query"]) for row in rows]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", default="duckdb")
    parser.add_argument("--uri", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--data", required=True, help="directory of Parquet files")
    parser.add_argument("--queries", default="tpch,types")
    parser.add_argument("--make-type-matrix", action="store_true",
                        help="write type_matrix.parquet into --data and exit")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.make_type_matrix:
        path = os.path.join(args.data, "type_matrix.parquet")
        run_duckdb(args.duckdb, TYPE_MATRIX_SQL.replace("{path}", path))
        print(f"wrote {path}")
        return 0

    tables = sorted(
        os.path.splitext(name)[0]
        for name in os.listdir(args.data)
        if name.endswith(".parquet")
    )
    native_setup = "".join(
        f"CREATE VIEW {t} AS FROM read_parquet('{os.path.join(args.data, t)}.parquet');"
        for t in tables
    )
    remote_setup = (
        f"CREATE SECRET (TYPE quack, TOKEN '{args.token}');"
        f"ATTACH '{args.uri}' AS df; USE df;"
    )

    queries = []
    kinds = args.queries.split(",")
    if "tpch" in kinds:
        queries += tpch_queries(args.duckdb)
    if "types" in kinds:
        queries += [(f"types-{i:02d}", q) for i, q in enumerate(TYPE_QUERIES, 1)]

    failures = 0
    for name, query in queries:
        ordered = re.search(r"\border\s+by\b", query, re.IGNORECASE) is not None
        try:
            native = run_duckdb(args.duckdb, native_setup + query)
        except RuntimeError as error:
            failures += 1
            print(f"FAIL {name}: native DuckDB failed: {error}")
            continue
        try:
            remote = run_duckdb(args.duckdb, remote_setup + query)
        except RuntimeError as error:
            failures += 1
            print(f"FAIL {name}: {error}")
            continue
        if rows_equal(native, remote, ordered):
            print(f"ok   {name} ({len(native)} rows)")
        else:
            failures += 1
            print(f"FAIL {name}: results differ")
            if args.verbose:
                print("  native:", json.dumps(native[:5], default=str))
                print("  remote:", json.dumps(remote[:5], default=str))
    print(f"{len(queries) - failures}/{len(queries)} queries match")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
