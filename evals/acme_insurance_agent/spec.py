"""ACME Insurance benchmark spec — the dataset behind dbt's 2026
"Semantic Layer vs. Text-to-SQL" post (https://docs.getdbt.com/blog/semantic-layer-vs-text-to-sql-2026).

File-based, no scale factor: the DuckDB is built from the vendored CSVs
(``db_build.py``) and every question is scored against the benchmark's own
gold SQL (``references/``, DuckDB-adapted by ``build_catalog.py``). The
``sql_schema`` leg gets the same ``ACME_small.ddl`` the blog's text-to-SQL
agent saw, as ``schema.md``.
"""

from __future__ import annotations

import sys
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent

# The viewer imports this spec by file path (no package context), so the
# sibling db_build import needs the eval dir on sys.path explicitly.
if str(EVAL_DIR) not in sys.path:
    sys.path.insert(0, str(EVAL_DIR))

import db_build
from common.spec import BenchmarkSpec

SPEC = BenchmarkSpec(
    name="ACME Insurance",
    short_name="acme",
    duckdb_extension="",
    generator_sql="",
    db_filename=db_build.DB_FILENAME,
    eval_dir=EVAL_DIR,
    prompts_file=EVAL_DIR / "query_prompts.json",
    references_dir=EVAL_DIR / "references",
    schema_md_file=EVAL_DIR / "schema.md",
    database_builder=db_build.build_database,
    default_enriched_dir=EVAL_DIR / "enriched_model",
    default_scale_factor=1.0,
    dataset_note=(
        "This dataset is a small hand-built sample, not a scale-factor "
        "generator: each table holds a handful of rows, so small counts and "
        "one-row answers are normal. Once your query runs and returns the "
        "requested shape it is done; do not spend calls probing the data volume."
    ),
    # The eleven blog questions lead the catalog; --num-queries 43 runs the
    # whole ACME set.
    default_num_queries=11,
)
