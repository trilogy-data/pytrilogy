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
`trilogy.toml`, NOT inside `raw/`. Each question below states its exact filename
(`queryNN.preql`, where NN is that question's number).

Import ONLY the fact table the question is about — `ingest --all` linked its
foreign keys, so its dimension tables are reached by chaining through it
(`store_returns.store.state`, `store_returns.date_dim.year`). Do NOT separately
import dimension tables — a separate import is a disconnected copy that will not
join. If a question genuinely spans two fact tables (e.g. store_sales and
inventory), import both and `merge` them on their shared dimension key — mark
the superset side with `~`: `merge inventory.item.id into ~store_sales.item.id;`
(a plain merge with no `~` asserts strict equivalence). Do not edit the `raw/`
model files. Read the fact's model file first (e.g. `read_file` on
`raw/store_returns.preql`) for exact concept names, then write a query like:

    import raw.store_returns as store_returns;

    where store_returns.store.state = 'TN'
      and store_returns.date_dim.year = 2000
    select
        store_returns.customer.customer_id,
        sum(store_returns.return_amt) as total_returns
    order by total_returns desc
    limit 100;

Each file must be a complete, runnable Trilogy query that returns the answer to
its question. Validate each one with `trilogy run <file>` before moving on to
the next.

Business questions
==================
{questions}
"""


def load_prompts() -> list[dict]:
    return json.loads(PROMPTS_FILE.read_text(encoding="utf-8"))


def selected_ids(num_queries: int) -> list[int]:
    """TPC-DS query ids the eval is running, in order."""
    return [p["id"] for p in load_prompts()[:num_queries]]


def build_task(num_queries: int) -> str:
    prompts = load_prompts()[:num_queries]
    questions = "\n\n".join(
        f"Question {p['id']} -> write `query{p['id']:02d}.preql`\n{p['prompt']}"
        for p in prompts
    )
    return TASK_TEMPLATE.format(n=len(prompts), questions=questions)
