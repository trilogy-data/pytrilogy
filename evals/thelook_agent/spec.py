"""Synthetic thelook benchmark for partial-bridge error recovery."""

from __future__ import annotations

import sys
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
if str(EVAL_DIR) not in sys.path:
    sys.path.insert(0, str(EVAL_DIR))

import db_build
from common.spec import BenchmarkSpec

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
