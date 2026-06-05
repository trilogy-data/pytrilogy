"""TPC-H benchmark spec. The bootstrap (sys.path / import shim) lives in the
per-mode scripts; this module just defines SPEC."""

from __future__ import annotations

from pathlib import Path

from common.spec import BenchmarkSpec

EVAL_DIR = Path(__file__).resolve().parent

SPEC = BenchmarkSpec(
    name="TPC-H",
    short_name="tpch",
    duckdb_extension="tpch",
    # dbgen is procedural (no result set to materialize).
    generator_sql="CALL dbgen(sf={sf})",
    db_filename="tpch.duckdb",
    eval_dir=EVAL_DIR,
    prompts_file=EVAL_DIR / "query_prompts.json",
    # tests/modeling/tpc_h has cache_warm helpers that aren't model files.
    enriched_skip_prefixes=("query", "adhoc", "cache"),
    default_scale_factor=0.1,
    default_num_queries=22,
    default_enriched_dir=EVAL_DIR.parents[1] / "tests" / "modeling" / "tpc_h",
)
