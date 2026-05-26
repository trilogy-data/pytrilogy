"""Assemble the natural-language task handed to the Trilogy agent.

Prompts are intentionally minimal — environment + deliverable + the question(s).
All query-authoring guidance (imports, chaining, function reference) lives in
the Trilogy language reference, which the agent loads via `trilogy agent-info`.
"""

from __future__ import annotations

import json

from .spec import BenchmarkSpec

_SINGLE_QUERY_TEMPLATE = """\
Trilogy project in this directory. `trilogy.toml` configures a DuckDB database
(`{db}`) already loaded with {bench} data, and `raw/` is already
populated with ingested Trilogy model files — do NOT re-run `trilogy ingest`
and do NOT edit files in `raw/`.

Answer the ONE business question below by writing a Trilogy query file to
`query{{nn}}.preql` in the working directory (alongside `trilogy.toml`, NOT
inside `raw/`). Validate with `trilogy run query{{nn}}.preql`. Return control
once it runs cleanly.

Question {{id}}:
{{prompt}}
"""

_TASK_TEMPLATE = """\
Trilogy project in this directory. `trilogy.toml` configures a DuckDB database
(`{db}`) already loaded with the {bench} benchmark schema and data.

Your goal:
1. Build a Trilogy semantic data model with `trilogy ingest --all` — writes
   one .preql model file per table under `raw/`. Do NOT overwrite files in
   `raw/` unless deliberately correcting a model definition.
2. Answer each of the {{n}} business questions below by writing a Trilogy query.

Write one query file per question alongside `trilogy.toml` (NOT inside `raw/`).
Each question below states its exact filename (`queryNN.preql`, where NN is the
question number). Validate each file with `trilogy run <file>` before moving
on.

Business questions
==================
{{questions}}
"""


def load_prompts(spec: BenchmarkSpec) -> list[dict]:
    return json.loads(spec.prompts_file.read_text(encoding="utf-8"))


def active_prompts(spec: BenchmarkSpec) -> list[dict]:
    """Prompts the eval runs. ``impossible``-graded entries stay in the catalog
    for the record but are excluded."""
    return [p for p in load_prompts(spec) if p.get("grade") != "impossible"]


def selected_ids(spec: BenchmarkSpec, num_queries: int) -> list[int]:
    return [p["id"] for p in active_prompts(spec)[:num_queries]]


def build_single_query_task(spec: BenchmarkSpec, entry: dict) -> str:
    template = _SINGLE_QUERY_TEMPLATE.format(db=spec.db_filename, bench=spec.name)
    return template.format(
        id=entry["id"],
        nn=f"{entry['id']:02d}",
        prompt=entry["prompt"],
    )


def build_task(spec: BenchmarkSpec, num_queries: int) -> str:
    prompts = active_prompts(spec)[:num_queries]
    questions = "\n\n".join(
        f"Question {p['id']} -> write `query{p['id']:02d}.preql`\n{p['prompt']}"
        for p in prompts
    )
    template = _TASK_TEMPLATE.format(db=spec.db_filename, bench=spec.name)
    return template.format(n=len(prompts), questions=questions)
