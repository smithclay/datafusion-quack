#!/usr/bin/env python3
"""The DuckDB ATTACH scenario (milestone M4) against a seeded datafusion-quack.

    attach.py --duckdb duckdb --uri quack:127.0.0.1:9494 --token T

The server must run with `--seed provider-fixtures`. Each step runs in a fresh DuckDB
process that ATTACHes the server as `df`, and checks the rows that come back.
"""

import argparse
import sys

import duckdb_cli


def run(args, sql):
    setup = f"CREATE SECRET (TYPE quack, TOKEN '{args.token}'); ATTACH '{args.uri}' AS df;"
    try:
        return duckdb_cli.run(args.duckdb, setup + sql)
    except RuntimeError as error:
        raise AssertionError(f"{sql}: {error}") from error


def fails(args, sql):
    """Runs `sql`, which must fail; returns DuckDB's error message."""
    try:
        run(args, sql)
    except AssertionError as error:
        return str(error)
    raise AssertionError(f"{sql}: expected an error")


def check(name, actual, expected):
    if actual != expected:
        raise AssertionError(f"{name}:\n  expected {expected}\n  got      {actual}")
    print(f"ok   {name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", default="duckdb")
    parser.add_argument("--uri", required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()

    tables = run(args, "SELECT name FROM (SHOW ALL TABLES) WHERE database = 'df' ORDER BY name")
    names = [row["name"] for row in tables]
    # remote views are queryable but not listed, as with DuckDB's own quack_serve
    for table in ["MixedCase_seed", "quack_nullability", "quack_pushdown", "quack_types"]:
        if table not in names:
            raise AssertionError(f"SHOW ALL TABLES has no {table}: {names}")
    print("ok   SHOW ALL TABLES")

    describe = run(args, "SELECT column_name, column_type, \"null\" FROM (DESCRIBE df.quack_nullability)")
    check(
        "DESCRIBE",
        [(r["column_name"], r["column_type"], r["null"]) for r in describe],
        [("id", "INTEGER", "NO"), ("name", "VARCHAR", "NO"), ("note", "VARCHAR", "YES"),
         ("amount", "DECIMAL(10,2)", "NO")],
    )

    rows = run(args, "SELECT id, s FROM df.quack_pushdown WHERE i > 1 AND d < DATE '2024-12-01' ORDER BY id")
    check("filter", rows, [{"id": 2, "s": "X"}, {"id": 5, "s": "Xylophone"}])

    rows = run(args, "SELECT c.name, sum(o.amount) AS total FROM df.quack_orders o "
                     "JOIN df.quack_customers c ON o.customer_id = c.id GROUP BY ALL ORDER BY 1")
    check("join and aggregate", [(r["name"], int(r["total"])) for r in rows],
          [("ann", 90), ("bo", 120), ("cy", 150)])

    rows = run(args, "SELECT count(*) AS n, max(k) AS m FROM df.quack_exhaust")
    check("count over 1M rows", rows, [{"n": 1000000, "m": 999999}])

    rows = run(args, "SELECT k FROM df.quack_exhaust ORDER BY k DESC LIMIT 2")
    check("top-k", rows, [{"k": 999999}, {"k": 999998}])

    rows = run(args, "FROM df.quack_view")
    check("view", rows, [{"id": 7}])

    rows = run(args, "FROM df.mixedcase_seed")
    check("case-insensitive name", rows, [{"id": 7}])

    rows = run(args, "SELECT c_dec38, c_ts_ns, c_list, c_struct, c_map FROM df.quack_types WHERE c_bool")
    check("nested and wide types", rows, [{
        "c_dec38": "1234567890123456789012345678.0123456789",
        "c_ts_ns": "2024-02-29 12:34:56.123456789",
        "c_list": [1, None, 3],
        "c_struct": {"a": 1, "b": "z"},
        "c_map": {"k1": 1, "k2": None},
    }])

    rows = run(args, "BEGIN; SELECT count(*) AS n FROM df.quack_ext_a; COMMIT;")
    check("explicit transaction", rows, [{"n": 3}])

    # writes: DuckDB rewrites these before sending them (INSERT ... (VALUES ...))
    run(args, "CREATE TABLE df.attach_writes (id INTEGER, name VARCHAR)")
    run(args, "INSERT INTO df.attach_writes VALUES (1, 'a'), (2, 'b')")
    run(args, "INSERT INTO df.attach_writes (name, id) VALUES ('c', 3)")
    run(args, "INSERT INTO df.attach_writes SELECT id + 10, name FROM df.attach_writes WHERE id = 1")
    run(args, "UPDATE df.attach_writes SET name = 'z' WHERE id = 1")
    run(args, "DELETE FROM df.attach_writes WHERE id = 2")
    rows = run(args, "FROM df.attach_writes ORDER BY id")
    check("insert, update and delete", rows,
          [{"id": 1, "name": "z"}, {"id": 3, "name": "c"}, {"id": 11, "name": "a"}])
    run(args, "CREATE TABLE df.attach_copy AS SELECT * FROM df.attach_file")
    run(args, "INSERT INTO df.attach_copy VALUES (2, 'copy')")
    rows = run(args, "SELECT count(*) AS n FROM df.attach_copy")
    check("a writable copy of a file table", rows, [{"n": 2}])
    error = fails(args, "INSERT INTO df.attach_file VALUES (2, 'b')")
    if "CREATE TABLE" not in error:
        raise AssertionError(f"a write to a file table should say how to copy it: {error}")
    print("ok   writes to a file table are refused")
    rows = run(args, "SELECT count(*) AS n FROM df.attach_file")
    check("the file table is unchanged", rows, [{"n": 1}])
    # the provider suite uses this server next
    run(args, "DROP TABLE df.attach_writes; DROP TABLE df.attach_copy")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as error:
        print(f"FAIL {error}")
        sys.exit(1)
