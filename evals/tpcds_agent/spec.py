"""TPC-DS benchmark spec. The bootstrap (sys.path / import shim) lives in the
per-mode scripts; this module just defines SPEC."""

from __future__ import annotations

from pathlib import Path

from common.spec import BenchmarkSpec

EVAL_DIR = Path(__file__).resolve().parent

SPEC = BenchmarkSpec(
    name="TPC-DS",
    short_name="tpcds",
    duckdb_extension="tpcds",
    # dsdgen is a lazy table function — common.db.build_database materializes
    # SELECT-shaped generators via fetchall and runs CALL-shaped ones directly.
    generator_sql="SELECT * FROM dsdgen(sf={sf})",
    db_filename="tpcds.duckdb",
    eval_dir=EVAL_DIR,
    prompts_file=EVAL_DIR / "query_prompts.json",
    enriched_skip_prefixes=("query", "adhoc"),
    # tests/modeling/tpc_ds_duckdb is the hand-curated semantic model the
    # enriched leg of `--both-modes` seeds from instead of `trilogy ingest --all`.
    default_enriched_dir=EVAL_DIR.parents[1] / "tests" / "modeling" / "tpc_ds_duckdb",
    default_scale_factor=0.01,
    default_num_queries=20,
)
