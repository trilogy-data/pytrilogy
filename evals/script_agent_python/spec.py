from __future__ import annotations

import sys
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(EVAL_DIR.parents[0]))

from common.spec import BenchmarkSpec
from script_agent.common import TASK_TEMPLATE, build_empty_database

SPEC = BenchmarkSpec(
    name="Python Script Datasource",
    short_name="script_python",
    duckdb_extension="",
    generator_sql="",
    db_filename="script_python.duckdb",
    eval_dir=EVAL_DIR,
    prompts_file=EVAL_DIR / "query_prompts.json",
    references_dir=EVAL_DIR / "references",
    database_builder=lambda: build_empty_database(EVAL_DIR, "script_python.duckdb"),
    default_num_queries=25,
    task_template=TASK_TEMPLATE,
    candidate_sidecar_extensions=(".py",),
    skip_model_setup=True,
    enable_python_datasources=True,
)
