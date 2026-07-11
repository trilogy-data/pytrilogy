"""Build and cache a DuckDB database loaded with benchmark data.

The database is generated once per (benchmark, scale_factor) and cached under
``<eval_dir>/.cache/``; each eval run gets its own copy so the agent cannot
corrupt the cached source.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from .spec import BenchmarkSpec


def cache_path(spec: BenchmarkSpec, scale_factor: float) -> Path:
    return spec.cache_dir / f"{spec.short_name}_sf{scale_factor:g}.duckdb"


def build_database(spec: BenchmarkSpec, scale_factor: float) -> Path:
    """Return a cached DuckDB file populated with the benchmark's data at
    ``scale_factor``, generating it via ``spec.duckdb_extension`` on first use.
    File-based benchmarks provide ``spec.database_builder`` instead."""
    import duckdb

    if spec.database_builder is not None:
        return spec.database_builder()

    spec.cache_dir.mkdir(parents=True, exist_ok=True)
    db_path = cache_path(spec, scale_factor)
    if db_path.exists():
        return db_path

    # Generate into a temp file so an interrupted build never leaves a
    # half-populated database parading as a valid cache entry.
    tmp_path = db_path.with_suffix(".building")
    tmp_path.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp_path))
    try:
        con.execute(f"INSTALL {spec.duckdb_extension}; LOAD {spec.duckdb_extension};")
        # dsdgen is a lazy table function (must be materialized via .fetchall);
        # dbgen is procedural (CALL); the spec carries whichever form applies.
        gen = spec.generator_sql.format(sf=scale_factor)
        if gen.strip().upper().startswith("SELECT"):
            con.execute(gen).fetchall()
        else:
            con.execute(gen)
        # Fold the WAL into the main file so the rename below moves a complete,
        # self-contained database.
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
