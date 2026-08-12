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


def generate_schema_md(db_path: Path) -> str:
    """Introspect ``db_path`` and return a markdown schema doc: one section per
    base table with its columns/types, row count, and any COMMENT ON metadata."""
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = con.execute(
            "select table_name, comment from duckdb_tables() "
            "where schema_name = 'main' and not internal order by table_name"
        ).fetchall()
        lines = [
            "# Database schema",
            "",
            "DuckDB database. Tables and columns below; write standard DuckDB SQL.",
            "",
        ]
        for table, table_comment in tables:
            cols = con.execute(
                "select column_name, data_type, comment from duckdb_columns() "
                "where schema_name = 'main' and table_name = ? "
                "order by column_index",
                [table],
            ).fetchall()
            try:
                row = con.execute(f'select count(*) from "{table}"').fetchone()
                row_count = row[0] if row else "?"
            except Exception:
                row_count = "?"
            lines.append(f"## {table} ({row_count} rows)")
            lines.append("")
            if table_comment:
                lines.append(table_comment)
                lines.append("")
            if any(comment for _, _, comment in cols):
                lines.append("| column | type | comment |")
                lines.append("|---|---|---|")
                for name, dtype, comment in cols:
                    lines.append(f"| {name} | {dtype} | {comment or ''} |")
            else:
                lines.append("| column | type |")
                lines.append("|---|---|")
                for name, dtype, _ in cols:
                    lines.append(f"| {name} | {dtype} |")
            lines.append("")
        return "\n".join(lines)
    finally:
        con.close()


def write_schema_md(
    db_path: Path,
    dest: Path,
    override: Path | None = None,
) -> Path:
    """Write ``schema.md`` to ``dest``. Uses the curated ``override`` file when
    it exists, otherwise auto-generates from ``db_path``."""
    if override is not None and override.exists():
        shutil.copy2(override, dest)
    else:
        dest.write_text(generate_schema_md(db_path), encoding="utf-8")
    return dest
