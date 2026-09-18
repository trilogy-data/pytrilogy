"""DABstep benchmark spec (https://huggingface.co/datasets/adyen/DABstep).

File-based payments dataset — no generator extension, no scale factor. The
DuckDB is built from ``data/context`` (run ``download_data.py`` first) and
every question is scored against a canonical reference SQL in ``references/``,
validated against the published dev-split answers (see query_prompts.json).
"""

from __future__ import annotations

from pathlib import Path

from common.siblings import load_sibling
from common.spec import BenchmarkSpec

EVAL_DIR = Path(__file__).resolve().parent

# By bare name every eval's db_build shares one sys.modules slot.
db_build = load_sibling(__file__, "db_build")

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
    # The manual carries the fee-matching semantics the questions hinge on.
    # The SQL baselines and the auto-ingest leg get the raw markdown; the
    # enriched leg gets ONLY the curated model, which must encode that
    # knowledge itself (concept docs + derived concepts) — the funnel then
    # compares agent+docs+db against agent+semantic-model.
    doc_files=db_build.DOC_FILES,
    doc_categories=("sql_bare", "sql_schema", "ingest"),
    docs_preamble=(
        "Domain docs in the working directory: manual.md (account types, ACI "
        "codes, and how fee rules match transactions), payments-readme.md "
        "(payments column reference), schema_notes.md (how the fee rules' "
        "list-fields map onto tables). Read the relevant sections before "
        "answering."
    ),
    default_enriched_dir=EVAL_DIR / "enriched_model",
    default_scale_factor=1.0,
    default_num_queries=10,
)
