"""Runs SQL through the DuckDB CLI and returns the last result as JSON rows."""

import json
import subprocess


def run(duckdb, sql):
    """Runs `sql` in a fresh DuckDB process; returns the last statement's rows.

    Raises RuntimeError when DuckDB reports an error.
    """
    result = subprocess.run(
        [duckdb, "-init", "/dev/null", "-json", "-c", sql],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0 or "Error" in result.stderr:
        raise RuntimeError((result.stderr or result.stdout).strip())
    # each statement that returns rows prints one JSON array; keep the last
    decoder, text, position, last = json.JSONDecoder(), result.stdout.strip(), 0, []
    while position < len(text):
        while position < len(text) and text[position].isspace():
            position += 1
        if position < len(text):
            last, position = decoder.raw_decode(text, position)
    return last
