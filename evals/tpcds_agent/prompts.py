"""Assemble the natural-language task handed to the Trilogy agent.

The task prompt is intentionally minimal — environment + deliverable + the
question(s). All query-authoring guidance (imports, chaining, merge, function
reference) belongs in the Trilogy language reference, which the agent loads via
`trilogy agent-info`. Keep this file's templates env-level only; do not
duplicate language rules here.
"""

from __future__ import annotations

import json
from pathlib import Path

PROMPTS_FILE = Path(__file__).parent / "query_prompts.json"

TASK_TEMPLATE = """\
Trilogy project in this directory. `trilogy.toml` configures a DuckDB database
(`tpcds.duckdb`) already loaded with the TPC-DS benchmark schema and data.

Your goal:
1. Build a Trilogy semantic data model with `trilogy ingest --all` — writes
   one .preql model file per table under `raw/`. Do NOT overwrite files in
   `raw/` unless deliberately correcting a model definition.
2. Answer each of the {n} business questions below by writing a Trilogy query.

Write one query file per question alongside `trilogy.toml` (NOT inside `raw/`).
Each question below states its exact filename (`queryNN.preql`, where NN is the
question number). Validate each file with `trilogy run <file>` before moving
on.

Business questions
==================
{questions}
"""


def load_prompts() -> list[dict]:
    return json.loads(PROMPTS_FILE.read_text(encoding="utf-8"))


def active_prompts() -> list[dict]:
    """Prompts the eval runs. 'impossible'-graded entries stay in the catalog
    for the record but are excluded (e.g. unsolvable at low scale factor)."""
    return [p for p in load_prompts() if p.get("grade") != "impossible"]


def selected_ids(num_queries: int) -> list[int]:
    """TPC-DS query ids the eval is running, in order."""
    return [p["id"] for p in active_prompts()[:num_queries]]


SINGLE_QUERY_TEMPLATE = """\
Trilogy project in this directory. `trilogy.toml` configures a DuckDB database
(`tpcds.duckdb`) already loaded with TPC-DS data, and `raw/` is already
populated with ingested Trilogy model files — do NOT re-run `trilogy ingest`
and do NOT edit files in `raw/`.

Answer the ONE business question below by writing a Trilogy query file to
`query{nn}.preql` in the working directory (alongside `trilogy.toml`, NOT
inside `raw/`). Validate with `trilogy run query{nn}.preql`. Return control
once it runs cleanly.

Question {id}:
{prompt}
"""


def build_single_query_task(entry: dict) -> str:
    """Per-query task: one question, fresh agent context, raw/ already populated."""
    return SINGLE_QUERY_TEMPLATE.format(
        id=entry["id"],
        nn=f"{entry['id']:02d}",
        prompt=entry["prompt"],
    )


def build_task(num_queries: int) -> str:
    prompts = active_prompts()[:num_queries]
    questions = "\n\n".join(
        f"Question {p['id']} -> write `query{p['id']:02d}.preql`\n{p['prompt']}"
        for p in prompts
    )
    return TASK_TEMPLATE.format(n=len(prompts), questions=questions)
