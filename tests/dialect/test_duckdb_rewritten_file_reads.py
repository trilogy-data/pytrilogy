"""A file-backed datasource is rewritten under a connection that already read
it: refresh reads an asset's own parquet, writes a replacement and reads it
back. DuckDB's external file cache holds that path's blocks, and a same-size
rewrite within its mtime resolution is not caught by
`validate_external_file_cache`, so the second read gets the old bytes against
the new footer unless the write drops the cache.
"""

import duckdb

from trilogy import Dialects


def test_a_replaced_parquet_reads_fresh_on_the_same_connection(tmp_path):
    source = tmp_path / "source.parquet"
    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT 1 AS k, TIMESTAMP '2024-01-10 12:00:00' AS ts"
            " UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00')"
            f" TO '{source.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    asset = tmp_path / "asset.parquet"
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        "key k int; property k.ts datetime;"
        f" datasource src (k: k, ts: ts) grain (k) file `{source.as_posix()}`;"
    )
    copy = f"copy into parquet '{asset.as_posix()}' from select k, ts"
    executor.execute_text(copy + " where k = 1;")
    assert executor.execute_raw_sql(
        f"SELECT max(ts) FROM read_parquet('{asset.as_posix()}')"
    ).fetchall()

    executor.execute_text(copy + ";")

    assert executor.execute_raw_sql(
        f"SELECT k FROM read_parquet('{asset.as_posix()}') ORDER BY 1"
    ).fetchall() == [(1,), (2,)]


def test_the_cache_stays_on_for_reads():
    row = (
        Dialects.DUCK_DB.default_executor()
        .execute_raw_sql(
            "SELECT value FROM duckdb_settings()"
            " WHERE name = 'enable_external_file_cache'"
        )
        .fetchone()
    )
    assert row is None or str(row[0]).lower() == "true"
