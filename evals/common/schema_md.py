"""Render a workspace DuckDB's schema as markdown for the ``sql_schema``
no-Trilogy baseline.

The ``sql_bare`` category gives the agent only a database (it must discover the
schema itself via ``run_query('SHOW TABLES' / 'DESCRIBE ...')``); ``sql_schema``
additionally drops this generated ``schema.md`` into the workspace so the agent
starts with the table/column map. A curated doc can override it via
``BenchmarkSpec.schema_md_file``.
"""

from __future__ import annotations

import shutil
from pathlib import Path


def generate_schema_md(db_path: Path, benchmark_name: str) -> str:
    """Introspect ``db_path`` and return a markdown schema doc: one section per
    base table with its columns/types and row count."""
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = [
            row[0]
            for row in con.execute(
                "select table_name from information_schema.tables "
                "where table_schema = 'main' and table_type = 'BASE TABLE' "
                "order by table_name"
            ).fetchall()
        ]
        lines = [
            f"# {benchmark_name} database schema",
            "",
            "DuckDB database. Tables and columns below; write standard DuckDB SQL.",
            "",
        ]
        for table in tables:
            cols = con.execute(
                "select column_name, data_type from information_schema.columns "
                "where table_schema = 'main' and table_name = ? "
                "order by ordinal_position",
                [table],
            ).fetchall()
            try:
                row_count = con.execute(f'select count(*) from "{table}"').fetchone()[0]
            except Exception:
                row_count = "?"
            lines.append(f"## {table} ({row_count} rows)")
            lines.append("")
            lines.append("| column | type |")
            lines.append("|---|---|")
            for name, dtype in cols:
                lines.append(f"| {name} | {dtype} |")
            lines.append("")
        return "\n".join(lines)
    finally:
        con.close()


def write_schema_md(
    db_path: Path,
    benchmark_name: str,
    dest: Path,
    override: Path | None = None,
) -> Path:
    """Write ``schema.md`` to ``dest``. Uses the curated ``override`` file when
    it exists, otherwise auto-generates from ``db_path``."""
    if override is not None and override.exists():
        shutil.copy2(override, dest)
    else:
        dest.write_text(generate_schema_md(db_path, benchmark_name), encoding="utf-8")
    return dest
