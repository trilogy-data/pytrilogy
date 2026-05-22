"""Assemble the natural-language task handed to the Trilogy agent.

Steering is deliberately minimal: the agent gets the database location and the
business questions, and must discover the CLI workflow itself. The only
non-negotiable instructions are the deliverable filenames (so scoring can find
the output).
"""

from __future__ import annotations

import json
from pathlib import Path

PROMPTS_FILE = Path(__file__).parent / "query_prompts.json"

TASK_TEMPLATE = """\
This is a Trilogy data project. The working directory contains `trilogy.toml`,
which configures a DuckDB database named `tpcds.duckdb`. That database is already
loaded with the standard TPC-DS benchmark schema and data (tables such as
store_sales, customer, date_dim, item, and so on).

Your goal:
1. Build a Trilogy semantic data model: run `trilogy ingest --all`, which writes
   one .preql model file per table into a `raw/` directory. (`trilogy database
   list` shows the tables.) Do NOT overwrite files in `raw/` unless you are
   deliberately correcting a model definition.
2. Answer each of the {n} business questions below by writing a Trilogy query.

`ingest --all` infers foreign keys and links the datasources, so the generated
model can be queried across tables directly. If a specific join is still
missing, you may edit the `raw/` model files to bind a foreign-key column to the
referenced table's key concept.

Write one query file per question in the working directory itself — alongside
`trilogy.toml`, NOT inside `raw/` — named with a zero-padded index:
`query01.preql`, `query02.preql`, ... `query{nn:02d}.preql`.

A query imports the datasources it needs from the `raw/` model and selects from
them. Read a model file first (e.g. `read_file` on `raw/store_returns.preql`)
to learn its exact concept names, then write a query like:

    import raw.store_returns as store_returns;

    select
        store_returns.<some_concept>,
        sum(store_returns.<some_measure>) -> total
    order by total desc
    limit 100;

Each file must be a complete, runnable Trilogy query that returns the answer to
its question. Validate each one with `trilogy run query01.preql` before moving
on to the next.

Business questions
==================
{questions}
"""


def load_prompts() -> list[dict]:
    return json.loads(PROMPTS_FILE.read_text(encoding="utf-8"))


def build_task(num_queries: int) -> str:
    prompts = load_prompts()[:num_queries]
    questions = "\n\n".join(
        f"Question {p['id']} -> write `query{p['id']:02d}.preql`\n{p['prompt']}"
        for p in prompts
    )
    return TASK_TEMPLATE.format(n=num_queries, nn=num_queries, questions=questions)
