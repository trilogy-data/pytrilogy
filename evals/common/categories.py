"""Eval categories — the four ways an agent can be asked the same business
question, ordered by how much scaffolding the agent is given:

    sql_bare   — just a DuckDB database; the agent writes plain SQL and
                 discovers the schema itself.
    sql_schema — same, plus a generated ``schema.md`` table/column map.
    ingest     — an auto-ingested Trilogy semantic model (``raw/*.preql``).
    enriched   — a hand-curated Trilogy model.

The first two are no-Trilogy baselines (``--toolset sql``); the last two use the
Trilogy CLI. A category bundles everything that differs per leg: the agent
toolset (``harness``), the scored candidate extension, the per-query task text,
and the workspace ``setup`` step. The funnel report reads them in ``FUNNEL_ORDER``
to show the cumulative value each layer adds.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from . import agent_runner, prompts, schema_md
from .spec import BenchmarkSpec


@dataclass(frozen=True)
class Category:
    key: str
    label: str
    harness: str  # "trilogy" | "sql"
    candidate_ext: str  # ".preql" | ".sql"
    setup: Callable[..., dict]
    """setup(workspace, spec, *, db_path, enriched_dir) -> result dict
    (same shape as agent_runner.run_pre_ingest: exit_code/duration/stdout/stderr)."""

    def build_task(
        self, spec: BenchmarkSpec, entry: dict, include_docs: bool = False
    ) -> str:
        if include_docs and spec.docs_preamble:
            entry = {**entry, "prompt": f"{spec.docs_preamble}\n\n{entry['prompt']}"}
        if self.harness == "sql":
            return prompts.build_single_query_task_sql(spec, entry)
        return prompts.build_single_query_task(spec, entry)


def _setup_ingest(
    workspace: Path, spec: BenchmarkSpec, *, db_path, enriched_dir
) -> dict:
    return agent_runner.run_pre_ingest(workspace)


def _setup_enriched(
    workspace: Path, spec: BenchmarkSpec, *, db_path, enriched_dir
) -> dict:
    src = Path(enriched_dir) if enriched_dir else spec.default_enriched_dir
    if src is None:
        raise ValueError(
            "enriched category needs --enriched-model-dir or a "
            "spec.default_enriched_dir."
        )
    return agent_runner.install_enriched_model(
        workspace, Path(src), spec.enriched_skip_prefixes
    )


def _setup_sql_bare(
    workspace: Path, spec: BenchmarkSpec, *, db_path, enriched_dir
) -> dict:
    return {
        "exit_code": 0,
        "duration": 0.0,
        "stdout": "sql_bare: database only, no model setup.\n",
        "stderr": "",
    }


def _setup_sql_schema(
    workspace: Path, spec: BenchmarkSpec, *, db_path, enriched_dir
) -> dict:
    start = time.perf_counter()
    dest = schema_md.write_schema_md(
        Path(db_path), spec.name, workspace / "schema.md", spec.schema_md_file
    )
    source = (
        f"curated {spec.schema_md_file}"
        if spec.schema_md_file and spec.schema_md_file.exists()
        else "auto-generated from DuckDB"
    )
    return {
        "exit_code": 0,
        "duration": time.perf_counter() - start,
        "stdout": f"sql_schema: wrote {dest.name} ({source}).\n",
        "stderr": "",
    }


CATEGORIES: dict[str, Category] = {
    "sql_bare": Category("sql_bare", "db-only", "sql", ".sql", _setup_sql_bare),
    "sql_schema": Category("sql_schema", "db+schema", "sql", ".sql", _setup_sql_schema),
    "ingest": Category("ingest", "ingest", "trilogy", ".preql", _setup_ingest),
    "enriched": Category("enriched", "enriched", "trilogy", ".preql", _setup_enriched),
}

# Order of increasing scaffolding — the funnel reads this to show marginal lift.
FUNNEL_ORDER: list[str] = ["sql_bare", "sql_schema", "ingest", "enriched"]


def get_category(key: str) -> Category:
    if key not in CATEGORIES:
        raise ValueError(f"unknown category {key!r}; known: {', '.join(CATEGORIES)}.")
    return CATEGORIES[key]
