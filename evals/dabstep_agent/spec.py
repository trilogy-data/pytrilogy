"""DABstep benchmark spec (https://huggingface.co/datasets/adyen/DABstep).

File-based payments dataset — no generator extension, no scale factor. The
DuckDB is built from ``data/context`` (run ``download_data.py`` first) and
every question is scored against a canonical reference SQL in ``references/``,
validated against the published dev-split answers (see query_prompts.json).
"""

from __future__ import annotations

import sys
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent

# The viewer imports this spec by file path (no package context), so the
# sibling db_build import needs the eval dir on sys.path explicitly.
if str(EVAL_DIR) not in sys.path:
    sys.path.insert(0, str(EVAL_DIR))

import db_build  # noqa: E402
from common.spec import BenchmarkSpec  # noqa: E402

SPEC = BenchmarkSpec(
    name="DABstep",
    short_name="dabstep",
    duckdb_extension="",
    generator_sql="",
    db_filename=db_build.DB_FILENAME,
    eval_dir=EVAL_DIR,
    prompts_file=EVAL_DIR / "query_prompts.json",
    references_dir=EVAL_DIR / "references",
    database_builder=db_build.build_database,
    # The manual carries the fee-matching semantics the questions hinge on —
    # agents must be able to read it.
    doc_files=db_build.DOC_FILES,
    allow_file_read=True,
    default_scale_factor=1.0,
    default_num_queries=10,
)
