"""Synthetic thelook benchmark for partial-bridge error recovery."""

from __future__ import annotations

from pathlib import Path

from common.siblings import load_sibling
from common.spec import BenchmarkSpec

EVAL_DIR = Path(__file__).resolve().parent

# By bare name every eval's db_build shares one sys.modules slot.
db_build = load_sibling(__file__, "db_build")

SPEC = BenchmarkSpec(
    name="thelook Partial Bridge",
    short_name="thelook",
    duckdb_extension="",
    generator_sql="",
    db_filename=db_build.DB_FILENAME,
    eval_dir=EVAL_DIR,
    prompts_file=EVAL_DIR / "query_prompts.json",
    references_dir=EVAL_DIR / "references",
    database_builder=db_build.build_database,
    default_enriched_dir=EVAL_DIR / "enriched_model",
    default_scale_factor=1.0,
    default_num_queries=12,
)
