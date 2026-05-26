"""Build and cache a DuckDB database loaded with TPC-H data.

The database is generated once per scale factor and cached under ``.cache/``;
each eval run gets its own copy so the agent cannot corrupt the cached source.
"""

from __future__ import annotations

import shutil
from pathlib import Path


def cache_path(cache_dir: Path, scale_factor: float) -> Path:
    return cache_dir / f"tpch_sf{scale_factor:g}.duckdb"


def build_database(scale_factor: float, cache_dir: Path) -> Path:
    """Return a cached DuckDB file populated with TPC-H at ``scale_factor``,
    generating it via the duckdb ``tpch`` extension on first use."""
    import duckdb

    cache_dir.mkdir(parents=True, exist_ok=True)
    db_path = cache_path(cache_dir, scale_factor)
    if db_path.exists():
        return db_path

    # Generate into a temp file so an interrupted build never leaves a
    # half-populated database parading as a valid cache entry.
    tmp_path = db_path.with_suffix(".building")
    tmp_path.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp_path))
    try:
        con.execute("INSTALL tpch; LOAD tpch;")
        # dbgen is a procedural call (unlike tpcds's dsdgen lazy table function).
        con.execute(f"CALL dbgen(sf={scale_factor})")
        con.execute("CHECKPOINT;")
    finally:
        con.close()
    tmp_path.with_name(tmp_path.name + ".wal").unlink(missing_ok=True)
    tmp_path.replace(db_path)
    return db_path


def copy_database(src: Path, dest: Path) -> Path:
    """Copy the cached database to a per-run location."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return dest
