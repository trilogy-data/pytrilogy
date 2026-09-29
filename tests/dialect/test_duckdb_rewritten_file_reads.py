"""A file-backed datasource is rewritten under a connection that already read
it: refresh reads an asset's own parquet, writes a replacement and reads it
back. DuckDB's external file cache holds that path's blocks, and a same-size
rewrite within its mtime resolution is not caught by
`validate_external_file_cache`, so the second read gets the old bytes against
the new footer.
"""

import uuid
from pathlib import Path

import duckdb
from sqlalchemy import text

from trilogy.dialect.config import DuckDBConfig
from trilogy.dialect.enums import default_factory


def _write(path: Path, rows: str) -> None:
    con = duckdb.connect()
    try:
        con.execute(f"COPY ({rows}) TO '{path.as_posix()}' (FORMAT PARQUET)")
    finally:
        con.close()


def test_a_replaced_parquet_reads_fresh_on_the_same_connection(tmp_path):
    """The refresh shape: probe the asset's watermark, rebuild it from its
    source into a staged file, move that over the asset, read it back."""
    source = tmp_path / "source.parquet"
    _write(
        source,
        "SELECT 1 AS k, TIMESTAMP '2024-01-10 12:00:00' AS ts"
        " UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00'",
    )
    asset = tmp_path / "asset.parquet"
    _write(asset, f"SELECT * FROM read_parquet('{source.as_posix()}') WHERE k = 1")

    engine = default_factory(DuckDBConfig(), DuckDBConfig)
    with engine.connect() as conn:
        conn.execute(
            text(f"SELECT max(ts) FROM read_parquet('{asset.as_posix()}')")
        ).fetchall()

        staged = tmp_path / f"asset.parquet.{uuid.uuid4().hex[:8]}.tmp"
        conn.execute(
            text(
                f"COPY (SELECT * FROM read_parquet('{source.as_posix()}'))"
                f" TO '{staged.as_posix()}' (FORMAT PARQUET, USE_TMP_FILE false)"
            )
        )
        staged.replace(asset)

        assert conn.execute(
            text(f"SELECT k FROM read_parquet('{asset.as_posix()}') ORDER BY 1")
        ).fetchall() == [(1,), (2,)]
